package selectflow

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrTimeout = errors.New("истекло время ожидания")
	ErrClosed  = errors.New("канал закрыт")
)

// TODO: FirstReady должен дождаться готового канала и вернуть значение,
// название источника и true. Источник равен "left" или "right".
// Закрытый выбранный канал даёт пустую строку, название источника и false.
func FirstReady(left, right <-chan string) (string, string, bool) {
	select {
	case v, ok := <-left:
		if !ok {
			return "", "left", ok
		}
		return v, "left", ok
	case v, ok := <-right:
		if !ok {
			return "", "right", ok
		}
		return v, "right", ok
	}
}

// TODO: TrySend должен попытаться отправить value без ожидания.
// Если канал не готов принять значение, ветка default сразу возвращает false.
func TrySend(out chan<- int, value int) bool {
	select {
	case out <- value:
		return true
	default:
		return false
	}
}

// TODO: ReceiveOrTimeout должен вернуть значение из in, ErrClosed для закрытого in
// или ErrTimeout, когда первым готов канал тайм-аута.
func ReceiveOrTimeout(in <-chan string, timeout <-chan time.Time) (string, error) {
	select {
	case v, ok := <-in:
		if !ok {
			return "", ErrClosed
		}
		return v, nil
	case <-timeout:
		return "", ErrTimeout
	}
}

// TODO: TryReceive должен попытаться получить одно число без ожидания.
// Вернуть value, true только для реально полученного значения; nil, пустой
// или закрытый канал дают 0, false.
func TryReceive(in <-chan int) (int, bool) {
	select {
	case v, ok := <-in:
		if !ok {
			return 0, false
		}
		return v, true
	default:
		return 0, false
	}
}

// TODO: PrimaryOrFallback должен сначала проверить основной канал primary без ожидания.
// Если он не готов, нужно ждать значение из основного или запасного канала.
// В сигнатуре запасной канал называется fallback.
// Закрытый вход отключается; когда оба входа закрыты или равны nil,
// вернуть "", "", false.
func PrimaryOrFallback(primary, fallback <-chan string) (string, string, bool) {
	select {
	case v, ok := <-primary:
		if ok {
			return v, "primary", ok
		}
		primary = nil
	default:
	}

	for primary != nil || fallback != nil {
		select {
		case v, ok := <-primary:
			if ok {
				return v, "primary", ok
			}
			primary = nil
		case v, ok := <-fallback:
			if ok {
				return v, "fallback", ok
			}
			fallback = nil
		}
	}

	return "", "", false
}

func Example() string {
	left := make(chan string, 1)
	left <- "left-value"
	value, source, ok := FirstReady(left, nil)
	out := make(chan int, 1)
	sent := TrySend(out, 7)
	ready := make(chan string, 1)
	ready <- "ready"
	received, _ := ReceiveOrTimeout(ready, nil)
	return fmt.Sprintf("first=%s:%s:%t\nsend=%t\nreceive=%s", source, value, ok, sent, received)
}
