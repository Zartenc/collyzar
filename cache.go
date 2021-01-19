package collyzar

import "sync"

var lock sync.Mutex

type zarQueue struct {
	queue   []interface{}
	maxSize int
}

func (z *zarQueue) push(info interface{}) {
	lock.Lock()
	defer lock.Unlock()
	z.queue = append(z.queue, info)
}
func (z *zarQueue) pop() interface{} {
	lock.Lock()
	defer lock.Unlock()
	if len(z.queue) == 0 {
		return nil
	}
	if len(z.queue) >= 1 {
		var x interface{}
		x, z.queue = z.queue[0], z.queue[1:]
		return x
	}
	return nil

}
func (z *zarQueue) isFull() bool {
	lock.Lock()
	defer lock.Unlock()
	if len(z.queue) >= z.maxSize {
		return true
	} else {
		return false
	}
}

