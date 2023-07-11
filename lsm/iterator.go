package lsm

import (
	"zhangkv/utils"
)

type Iterator struct {
	it    Item
	iters []utils.Iterator
}

type Item struct {
	e *utils.Entry
}

func (it Item) Entry() *utils.Entry {
	return it.e
}
