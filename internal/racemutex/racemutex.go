package racemutex

import (
	"fmt"
	"sync"
)

type Counter struct {
	mu    sync.Mutex
	value int
}

// TODO: Add должен безопасно прибавить delta к общему значению.
// Если получатель метода равен nil, вызов нужно спокойно проигнорировать.
func (c *Counter) Add(delta int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.value += delta
	c.mu.Unlock()
}

// TODO: Value должен безопасно вернуть текущее значение.
// Если получатель метода равен nil, нужно вернуть 0.
func (c *Counter) Value() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

// TODO: ParallelAdd должен запустить количество рабочих горутин, заданное параметром workers.
// Каждая горутина выполняет increments вызовов Add(1); затем нужно дождаться всех и вернуть итог.
// Неположительные workers или increments дают 0.
func ParallelAdd(workers, increments int) int {
	var c Counter
	var wg sync.WaitGroup
	if workers <= 0 || increments <= 0 {
		return 0
	}
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range increments {
				c.Add(1)
			}
		}()
	}
	wg.Wait()
	return c.Value()
}

// TODO: Swap должен под одной блокировкой заменить значение и вернуть прежнее.
// Если получатель метода равен nil, вернуть 0 и ничего не делать.
func (c *Counter) Swap(value int) int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	beforeSwap := c.value
	c.value = value
	return beforeSwap
}

// TODO: ParallelDeltas должен запустить отдельную горутину для каждого delta,
// безопасно применить все изменения к одному Counter, дождаться их и вернуть итог.
func ParallelDeltas(deltas []int) int {
	var c Counter
	var wg sync.WaitGroup
	for _, v := range deltas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Add(v)
		}()
	}
	wg.Wait()
	return c.Value()
}

func Example() string {
	return fmt.Sprintf("counter=%d", ParallelAdd(8, 50))
}
