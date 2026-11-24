package waitgroup

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// TODO: RunAll должен запустить каждую ненулевую задачу в отдельной горутине
// и вернуться только после завершения всех задач.
func RunAll(tasks []func()) {
	var wg sync.WaitGroup
	for _, task := range tasks {
		if task == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			task()
		}()
	}
	wg.Wait()
}

// TODO: Squares должен вычислить квадраты конкурентно и вернуть их в исходном порядке.
// nil и пустой вход дают пустой ненулевой слайс.
func Squares(values []int) []int {
	if len(values) == 0 {
		return []int{}
	}
	var wg sync.WaitGroup
	for i, value := range values {
		wg.Add(1)
		go func() {
			defer wg.Done()
			values[i] = value * value
		}()
	}
	wg.Wait()
	return values
}

// TODO: ApplyAll должен применить fn к элементам конкурентно и сохранить порядок.
// Если fn == nil, верни независимую копию values.
func ApplyAll(values []int, fn func(int) int) []int {
	copyValues := make([]int, len(values))
	copy(copyValues, values)
	if fn == nil {
		return copyValues
	}
	var wg sync.WaitGroup
	for i, value := range values {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copyValues[i] = fn(value)
		}()
	}
	wg.Wait()
	return copyValues
}

// TODO: SumParts должен запустить отдельную горутину для каждой части,
// посчитать сумму внутри части и сохранить суммы в исходном порядке частей.
func SumParts(parts [][]int) []int {
	sums := make([]int, len(parts))
	var wg sync.WaitGroup
	for i, part := range parts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sum := 0
			for _, item := range part {
				sum += item
			}
			sums[i] = sum
		}()
	}
	wg.Wait()
	return sums
}

// TODO: CountMatches должен конкурентно посчитать количество target в каждой
// группе строк и вернуть счётчики в исходном порядке групп.
func CountMatches(groups [][]string, target string) []int {
	counts := make([]int, len(groups))
	var wg sync.WaitGroup
	for i, group := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counter := 0
			for _, item := range group {
				if item == target {
					counter++
				}
			}
			counts[i] = counter
		}()
	}
	wg.Wait()
	return counts
}

func Example() string {
	var count atomic.Int32
	RunAll([]func(){func() { count.Add(1) }, func() { count.Add(1) }})
	return fmt.Sprintf("tasks=%d\nsquares=%v\napplied=%v", count.Load(), Squares([]int{2, 3}), ApplyAll([]int{1, 2}, func(v int) int { return v + 10 }))
}
