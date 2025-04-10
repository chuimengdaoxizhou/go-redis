package wait

import (
	"sync"
	"time"
)

type Wait struct {
	wg sync.WaitGroup
}

func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

func (w *Wait) Done() {
	w.wg.Done()
}

func (w *Wait) Wait() {
	w.wg.Wait()
}
func (w *Wait) WaitWithTimeOut(timeout time.Duration) bool {
	// Create a channel to signal when the timeout occurs
	c := make(chan bool, 1)
	go func() {
		defer close(c)
		w.wg.Wait()
		c <- true
	}()

	// Wait for either the condition to be met or the timeout to occur
	select {
	case <-c:
		return false

	case <-time.After(timeout):
		return true
	}
}
