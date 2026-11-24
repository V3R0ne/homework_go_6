package closerange

import (
	"fmt"
	"sync"
)

// TODO: Generate должен в отдельной горутине отправить числа от 0 до limit-1,
// затем закрыть канал. Неположительный limit даёт сразу закрытый канал.
func Generate(limit int) <-chan int {
	if limit <= 0 {
		values := make(chan int)
		close(values)
		return values
	}
	values := make(chan int, limit)
	go func() {
		defer close(values)
		for i := range limit {
			values <- i
		}
	}()
	return values
}

// TODO: Collect должен прочитать канал циклом range до закрытия.
// Если отправитель не закроет канал, цикл будет ждать следующее значение.
// nil-канал трактуется как пустой вход; результат всегда ненулевой слайс.
func Collect(values <-chan int) []int {
	result := make([]int, 0)
	if values == nil {
		return []int{}
	}
	for v := range values {
		result = append(result, v)
	}
	return result
}

// TODO: FilterEven должен отправить только чётные значения в исходном порядке
// и закрыть выходной канал после завершения.
func FilterEven(values []int) <-chan int {
	result := make(chan int, len(values))
	for _, value := range values {
		if value%2 == 0 {
			result <- value
		}
	}
	close(result)
	return result
}

// TODO: Double должен в отдельной горутине отправить удвоенные значения
// в исходном порядке и закрыть выходной канал даже для пустого входа.
func Double(values []int) <-chan int {
	result := make(chan int, len(values))
	go func() {
		defer close(result)
		if len(values) == 0 {
			return
		}
		for _, v := range values {
			result <- v * 2
		}
	}()
	return result
}

// TODO: Merge должен объединить два входных канала в один и закрыть выход
// только после завершения обоих входов. nil-канал трактуется как пустой вход.
// Порядок внутри каждого входа сохраняется, общий порядок между входами не задан.
func Merge(left, right <-chan int) <-chan int {
	var wg sync.WaitGroup
	result := make(chan int)
	wg.Add(2)

	go func() {
		defer wg.Done()
		if left == nil {
			return
		}
		for v := range left {
			result <- v
		}
	}()

	go func() {
		defer wg.Done()
		if right == nil {
			return
		}
		for v := range right {
			result <- v
		}
	}()

	go func() {
		wg.Wait()
		close(result)
	}()
	return result
}

func Example() string {
	return fmt.Sprintf("generated=%v\neven=%v", Collect(Generate(5)), Collect(FilterEven([]int{1, 2, 3, 4, 5, 6})))
}
