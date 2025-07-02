package stack

import (
	"sync"
)

type Stack[T any] struct {
	lock  sync.Mutex
	items []T
}

func NewStack[T any]() *Stack[T] {
	return &Stack[T]{sync.Mutex{}, make([]T, 0)}
}

func (this *Stack[T]) Push(v T) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.items = append(this.items, v)
}

func (this *Stack[T]) Pop() (T, bool) {
	this.lock.Lock()
	defer this.lock.Unlock()

	l := len(this.items)
	if l == 0 {
		var empty T
		return empty, false
	}

	item := this.items[l-1]
	this.items = this.items[:l-1]
	return item, true
}

func (this *Stack[T]) Peek() (T, bool) {
	this.lock.Lock()
	defer this.lock.Unlock()

	l := len(this.items)
	if l == 0 {
		var empty T
		return empty, false
	}

	item := this.items[l-1]
	return item, true
}
