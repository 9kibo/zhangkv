package lsm

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"zhangkv/pb"
	"zhangkv/utils"
)

type keyRange struct {
	left  []byte
	right []byte
	inf   bool //标记，表示该keyRange的范围无限大
	size  int64
}

func (r keyRange) equals(dst keyRange) bool {
	return bytes.Equal(r.left, dst.left) &&
		bytes.Equal(r.right, dst.right) &&
		r.inf == dst.inf
}

var infRange = keyRange{inf: true}

func (r keyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.inf
}

func (lsm *LSM) newCompactStatus() *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < lsm.options.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}

// 归并优先级
type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            targets
}

// 归并目标
type targets struct {
	baseLevel int
	targetSz  []int64 //各个层级的预期大小
	fileSz    []int64
}

// 压缩计划
// 被压缩层 --> 压缩层
// 从被压缩层选择特定的table和压缩层中的table 进行合并，合并后添加到压缩层
type compactDef struct {
	compactorId int     //执行compactor的协程id
	t           targets //预期
	p           compactionPriority
	thisLevel   *levelHandler //被压缩层
	nextLevel   *levelHandler //压缩目标层

	top []*table //被压缩层选中table
	bot []*table //压缩层选择的table

	thisRange keyRange //被压缩层选择的key范围
	nextRange keyRange //压缩层选择的key范围
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

// runCompacter 启动一个compacter
func (lm *levelManager) runCompacter(id int) {
	defer lm.lsm.closer.Done()
	//随机时，将压缩协程时间间隔打乱，减少冲突
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	//阻塞协程
	case <-randomDelay.C:
	case <-lm.lsm.closer.CloseSignal:
		randomDelay.Stop()
		return
	}
	ticker := time.NewTicker(50000 * time.Millisecond)
	defer ticker.Stop()
	//按时间片进行压缩检测
	for {
		select {
		case <-ticker.C:
			lm.runOnce(id)
		case <-lm.lsm.closer.CloseSignal:
			return
		}
	}
}
func (lm *levelManager) runOnce(id int) bool {
	prios := lm.pickCompactLevels()
	if id == 0 {
		//0号携程倾向于压缩0层
		prios = moveL0toFront(prios)
	}
	for _, p := range prios {
		//按照优先级顺位执行
		if id == 0 && p.level == 0 {
			// 对于l0 无论得分多少都要运行
		} else if p.adjusted < 1.0 {
			// 对于其他level 如果等分小于 则不执行
			break
		}
		if lm.run(id, p) {
			return true
		}
	}
	return false
}

// pickCompactLevel 选择合适的level执行合并，返回判断的优先级
func (lm *levelManager) pickCompactLevels() (prios []compactionPriority) {
	t := lm.levelTargets()
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}
	// 根据l0表的table数量来对压缩提权 (当前层的table数量/零层文件的默认数量)
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLevelZeroTables))

	// 非l0 层都根据大小计算优先级
	for i := 1; i < len(lm.levels); i++ {
		// 处于压缩状态的sst 不能计算在内
		delSize := lm.compactState.delSize(i)
		l := lm.levels[i]
		sz := l.getTotalSize() - delSize
		// score的计算是 扣除正在合并的表后的尺寸与目标sz的比值
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	utils.CondPanic(len(prios) != len(lm.levels), errors.New("[pickCompactLevels] len(prios) != len(lm.levels)"))

	// 调整得分,为了维护LSMTree各level之间层级大小保持期望比例
	//PreAdjust/Adjust = PreSz / PreTar *  LevelTar / LevelSz
	//对于lsmTree而言保持稳定形态有助于减少读写放大
	//
	var prevLevel int
	for level := t.baseLevel; level < len(lm.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			//避免得分过大
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// 仅选择得分大于1的压缩内容，并且允许l0到l0的特殊压缩，为了提升查询性能允许l0层独自压缩
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// 按优先级排序,大->小
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

// delSize 获取指定层的预备删除的数据大小
func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}

func (lm *levelManager) levelTargets() targets {
	//闭包函数调整预期大小
	adjust := func(sz int64) int64 {
		if sz < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return sz
	}
	//初始化默认最大层，从下到上遍历
	t := targets{
		targetSz: make([]int64, len(lm.levels)),
		fileSz:   make([]int64, len(lm.levels)),
	}
	dbSize := lm.lastLevel().getTotalSize()
	for i := len(lm.levels) - 1; i > 0; i-- {
		levelTargetSize := adjust(dbSize)
		t.targetSz[i] = levelTargetSize
		// 如果当前level没有达到最低预期值,将当前level设为baselevel
		//compact更倾向于将数据压缩到更底层的level以减少写放大
		if t.baseLevel == 0 && levelTargetSize <= lm.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(lm.opt.LevelSizeMultiplier)
	}
	tsz := lm.opt.BaseTableSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			t.fileSz[i] = lm.opt.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}
	// 找到最后一个空level作为目标level实现跨level归并，减少写放大
	for i := t.baseLevel + 1; i < len(lm.levels)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	// 如果存在断层，则目标level++
	b := t.baseLevel
	lvl := lm.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}
func (lm *levelManager) lastLevel() *levelHandler {
	return lm.levels[len(lm.levels)-1]
}

// sortByStaleData 按表中陈旧数据的数量对sst文件进行排序
func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

func moveL0toFront(prios []compactionPriority) []compactionPriority {
	idx := -1
	for i, p := range prios {
		if p.level == 0 {
			idx = i
			break
		}
	}
	if idx > 0 {
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

// run 执行一个优先级指定的合并任务
func (lm *levelManager) run(id int, p compactionPriority) bool {
	err := lm.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
		// 什么也不做，此时合并过程被忽略
	default:
		log.Printf("[taskID:%d] While running doCompact: %v\n ", id, err)
	}
	return false
}

// doCompact 选择level中的某些表合并到目标level
func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	l := p.level
	utils.CondPanic(l >= lm.opt.MaxLevelNum, errors.New("[doCompact]check. l >= lm.opt.MaxLevelNum"))
	if p.t.baseLevel == 0 {
		p.t = lm.levelTargets()
	}
	//创建真正的压缩计划
	cd := compactDef{
		compactorId:  id,
		p:            p,
		t:            p.t,
		thisLevel:    lm.levels[l],
		dropPrefixes: p.dropPrefixes,
	}
	//如果是第0层，单独处理
	if l == 0 {
		cd.nextLevel = lm.levels[p.t.baseLevel]
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		// 如果不是最后一层，则压缩到下一层即可
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = lm.levels[l+1]
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
	}
	// 完成合并后 从合并状态中删除
	defer lm.compactState.delete(cd)
	// 执行合并计划
	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}
func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()
	tables := make([]*table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	//if last,doing maxLevel to maxLevel compaction
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}

	lm.sortByHeuristic(tables, cd)

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// 如果被压缩过了，则什么都不需要做
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}
		cd.top = []*table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)

		cd.bot = make([]*table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		if len(cd.bot) == 0 {
			cd.bot = []*table{}
			cd.nextRange = cd.thisRange
			if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

		if lm.compactState.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
			continue
		}
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false

}

// 根据版本号排序，旧数据在前
func (lm *levelManager) sortByHeuristic(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].ss.Indexs().MaxVersion < tables[j].ss.Indexs().MaxVersion
	})
}

// max level 自我压缩
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(sortedTables, cd)
	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		return false
	}
	cd.bot = []*table{}
	collectBotTables := func(t *table, needSz int64) {
		totalSize := t.Size()

		j := sort.Search(len(tables), func(i int) bool {
			return utils.CompareKeys(tables[i].ss.MinKey(), t.ss.MinKey()) >= 0
		})
		utils.CondPanic(tables[j].fid != t.fid, errors.New("tables[j].ID() != t.ID()"))
		j++
		// 估算压缩后的sst文件大小，sst文件过大会造成性能抖动
		for j < len(tables) {
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newT)
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		if now.Sub(*t.GetCreatedAt()) < time.Hour {
			//创建时间小于一小时
			continue
		}
		//脏数据文件不足10MB
		if t.StaleDataSize() < 10<<20 {
			continue
		}
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		cd.nextRange = cd.thisRange
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}
		cd.top = []*table{t}

		needFileSz := cd.t.fileSz[cd.thisLevel.levelNum]
		if t.Size() >= needFileSz {
			break
		}
		collectBotTables(t, needFileSz)
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}
	if len(cd.top) == 0 {
		return false
	}

	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0 先尝试从l0 到lbase的压缩，如果失败则对l0自己压缩
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return lm.fillTablesL0ToL0(cd)
}
func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("base level zero"))
	}
	//如果优先级低于1就不执行
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table
	var kr keyRange
	//从最老的文件开始
	for _, t := range top {
		dkr := getKeyRange(t)
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			break
		}
	}
	//获取range list的全局对象
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0ToL0 l0到l0压缩
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		// 只有0号压缩处理器可以执行，避免l0tol0的资源竞争
		return false
	}

	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	lm.compactState.Lock()
	defer lm.compactState.Unlock()

	top := cd.thisLevel.tables
	var out []*table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// 在L0 to L0 的压缩过程中，不要对过大的sst文件压缩，这会造成性能抖动
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			// 如果sst的创建时间不足10s 也不要回收
			continue
		}
		// 如果当前的sst 已经在压缩状态 也应该忽略
		if _, beingCompacted := lm.compactState.tables[t.fid]; beingCompacted {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		// 满足条件的sst小于4个那就不压缩了
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// 在这个过程中避免任何l0到其他层的合并
	thisLevel := lm.compactState.levels[cd.thisLevel.levelNum]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		lm.compactState.tables[t.fid] = struct{}{}
	}

	//  l0 to l0的压缩最终都会压缩为一个文件，这大大减少了l0层文件数量，减少了读放大
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

// 按照压缩计划执行
func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	//如果压缩时间过长，会打印信息，方便优化
	timeStart := time.Now()
	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel
	utils.CondPanic(len(cd.splits) != 0, errors.New("len(cd.splits) != 0"))
	if thisLevel == nextLevel {
		//lo to l0 and lMax to lMax 需要尽快完成，不做特殊处理
	} else {
		//对压缩计划启动多个协程并行压缩
		lm.addSplits(&cd)
	}
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}
	newTables, decr, err := lm.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	//资源回收
	defer func() {
		if decErr := decr(); err != nil {
			err = decErr
		}
	}()
	//manifest change
	changeSet := buildChangeSet(&cd, newTables)
	// 删除之前先更新manifest文件
	if err := lm.manifestFile.AddChanges(changeSet.Changes); err != nil {
		return err
	}

	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	defer decrRefs(cd.top)
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	//压缩时间超过两秒打印调试信息
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		fmt.Printf("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.levelNum, nextLevel.levelNum, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}
	return nil
}

// tablesToString
func tablesToString(tables []*table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, ".")
	return res
}

// compactBuildTables 合并两个层的sst文件
func (lm *levelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {
	topTables := cd.top
	botTables := cd.bot
	iterOpt := &utils.Options{
		IsAsc: true,
	}
	newIterator := func() []utils.Iterator {
		var iters []utils.Iterator
		switch {
		case lev == 0:
			//l0层
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		case len(topTables) > 0:
			iters = []utils.Iterator{topTables[0].NewIterator(iterOpt)}
		}
		return append(iters, NewConcatIterator(botTables, iterOpt))
	}
	//开始执行压缩任务
	res := make(chan *table, 3)
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits)) //限流器，限制并行压缩的协程数量
	for _, kr := range cd.splits {
		if err := inflightBuilders.Do(); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
		//开启协程处理子压缩
		go func(kr keyRange) {
			//资源回收
			defer inflightBuilders.Done(nil)
			it := NewMergeIterator(newIterator(), false)
			defer it.Close()
			lm.subcompact(it, kr, cd, inflightBuilders, res)
		}(kr)
	}
	var newTables []*table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()
	//等待所有压缩过程完成
	err := inflightBuilders.Finish()
	close(res)
	wg.Wait() //等待所有builder刷盘
	if err == nil {
		err = utils.SyncDir(lm.opt.WorkDir)
	}
	if err != nil {
		// 如果出现错误，则删除索引新创建的文件
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}
	sort.Slice(newTables, func(i, j int) bool {
		return utils.CompareKeys(newTables[i].ss.MaxKey(), newTables[j].ss.MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

// 真正执行并行压缩的子压缩文件
func (lm *levelManager) subcompact(it utils.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *table) {
	var lastKey []byte
	discardStats := make(map[uint32]int64)
	defer func() {
		lm.updateDiscardStats(discardStats)
	}()
	updateStats := func(e *utils.Entry) {
		if e.Meta&utils.BitValuePointer > 0 {
			var vp utils.ValuePtr
			vp.Decode(e.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}
	addKeys := func(builder *tableBuilder) {
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			key := it.Item().Entry().Key
			isExpired := IsDeletedOrExpired(it.Item().Entry())
			if !utils.SameKey(key, lastKey) {
				// 如果迭代器返回的key大于当前key的范围就不用执行了
				if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// 如果超过预估的sst文件大小，则直接结束
					break
				}
				// 把当前的key变为 lastKey
				lastKey = utils.SafeCopy(lastKey, key)
				//umVersions = 0
				// 如果左边界没有，则当前key给到左边界
				if len(tableKr.left) == 0 {
					tableKr.left = utils.SafeCopy(tableKr.left, key)
				}
				// 更新右边界
				tableKr.right = lastKey
			}
			// 判断是否是过期内容，是的话就删除
			switch {
			case isExpired:
				updateStats(it.Item().Entry())
				builder.AddStaleKey(it.Item().Entry())
			default:
				builder.AddKey(it.Item().Entry())
			}
		}
	}
	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		//初始化
		it.Rewind()
	}
	//循环调用
	for it.Valid() {
		key := it.Item().Entry().Key
		if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
			break
		}
		// 拼装table创建的参数
		builder := newTableBuilerWithSSTSize(lm.opt, cd.t.fileSz[cd.nextLevel.levelNum])

		addKeys(builder)

		if builder.empty() {
			builder.finish()
			builder.Close()
			continue
		}
		if err := inflightBuilders.Do(); err != nil {
			break
		}
		// 充分发挥 ssd的并行 写入特性
		go func(builder *tableBuilder) {
			defer inflightBuilders.Done(nil)
			defer builder.Close()
			var tbl *table
			newFID := atomic.AddUint64(&lm.maxFID, 1) // compact的时候是没有memtable的，这里自增maxFID即可。
			sstName := utils.FileNameSSTable(lm.opt.WorkDir, newFID)
			tbl = openTable(lm, sstName, builder)
			if tbl == nil {
				return
			}
			res <- tbl
		}(builder)
	}
}
func buildChangeSet(cd *compactDef, newTables []*table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes, newCreateChange(table.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.fid))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

// 判断是否过期 是可删除
func IsDeletedOrExpired(e *utils.Entry) bool {
	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}
func (lm *levelManager) updateDiscardStats(discardStats map[uint32]int64) {
	select {
	case *lm.lsm.options.DiscardStatsCh <- discardStats:
	default:
	}
}
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte) {
		skr.right = utils.Copy(right)
		cd.splits = append(cd.splits, skr)
		skr.left = skr.right
	}
	for i, t := range cd.bot {
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			right := utils.KeyWithTs(utils.ParseKey(t.ss.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}
}

// getKeyRange 返回一组sst的区间合并后的最大与最小值
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	minKey := tables[0].ss.MinKey()
	maxKey := tables[0].ss.MaxKey()
	for i := 1; i < len(tables); i++ {
		if utils.CompareKeys(tables[i].ss.MinKey(), minKey) < 0 {
			minKey = tables[i].ss.MinKey()
		}
		if utils.CompareKeys(tables[i].ss.MaxKey(), maxKey) > 0 {
			maxKey = tables[i].ss.MaxKey()
		}
	}
	return keyRange{
		left:  utils.KeyWithTs(utils.ParseKey(minKey), math.MaxUint64),
		right: utils.KeyWithTs(utils.ParseKey(maxKey), 0),
	}
}
func (r *keyRange) overlapsWith(dst keyRange) bool {
	//空keyRang总是重叠的
	if r.isEmpty() {
		return true
	}
	if dst.isEmpty() {
		return false
	}
	if r.inf || dst.inf {
		return true
	}
	if utils.CompareKeys(r.left, dst.right) > 0 {
		return false
	}
	if utils.CompareKeys(r.right, dst.left) < 0 {
		return false
	}
	// overlap
	return true
}

// 合并kr
func (r *keyRange) extend(kr keyRange) {
	if kr.isEmpty() {
		return
	}
	if r.isEmpty() {
		*r = kr
	}
	if len(r.left) == 0 || utils.CompareKeys(kr.left, r.left) < 0 {
		r.left = kr.left
	}
	if len(r.right) == 0 || utils.CompareKeys(kr.right, r.right) > 0 {
		r.right = kr.right
	}
	if kr.inf {
		r.inf = true
	}
}

// 层级压缩状态
type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

// 全局压缩状态
type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus //各个层级的压缩状态
	tables map[uint64]struct{}   //处于压缩状态的SSTable
}
type thisAndNextLevelRLocked struct{}

func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum
	utils.CondPanic(tl >= len(cs.levels), fmt.Errorf("Got level %d. Max levels: %d", tl, len(cs.levels)))
	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}
	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.fid] = struct{}{}
	}
	return true
}
func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}
func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum

	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	thisLevel.delSize -= cd.thisSize
	found := thisLevel.remove(cd.thisRange)
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevel.remove(cd.nextRange) && found
	}

	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: %s in this level %d.\n", this, tl)
		fmt.Printf("This Level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", next, cd.nextLevel.levelNum)
		fmt.Printf("Next Level:\n%s\n", nextLevel.debug())
		log.Fatal("keyRange not found")
	}
	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.fid]
		utils.CondPanic(!ok, fmt.Errorf("cs.tables is nil"))
		delete(cs.tables, t.fid)
	}
}
func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}
func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.left, r.right, r.inf)
}
func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}
func iteratorsReversed(th []*table, opt *utils.Options) []utils.Iterator {
	out := make([]utils.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}
func newDeleteChange(id uint64) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id: id,
		Op: pb.ManifestChange_DELETE,
	}
}

// newCreateChange
func newCreateChange(id uint64, level int) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:    id,
		Op:    pb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}
