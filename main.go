package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func generator(ctx context.Context, nums ...int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)

		for _, n := range nums {
			select {
			case <-ctx.Done():
				fmt.Println("Получили сигнал отмены...")
				return
			case out <- n:
			}
		}
	}()

	return out
}

func worker(ctx context.Context, cancel context.CancelFunc, in <-chan int, workerId int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		for n := range in {
			select {
			case <-ctx.Done():
				fmt.Printf("[Воркер %d] Контекст отменён\r\n", workerId)
				return
			default:
			}

			time.Sleep(300 * time.Millisecond)
			if n == 5 {
				fmt.Printf("[Воркер %d] поймал триггер!!! отменяем всё %d\r\n", workerId, n)
				cancel()
			}

			select {
			case <-ctx.Done():
				return
			case out <- n * n:
				fmt.Printf("[Воркер %d] Обрабатывает число: %d\r\n", workerId, n)
			}

		}
	}()
	return out
}

func fanIn(ctx context.Context, channels ...<-chan int) <-chan int {
	var wg sync.WaitGroup

	multiplexedStream := make(chan int)
	multiplex := func(c <-chan int) {
		defer wg.Done()
		for n := range c {
			select {
			case <-ctx.Done():
				return
			case multiplexedStream <- n:
			}
		}
	}

	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}

	go func() {
		wg.Wait()
		close(multiplexedStream)
	}()

	return multiplexedStream
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	inputChan := generator(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)

	w1 := worker(ctx, cancel, inputChan, 1)
	w2 := worker(ctx, cancel, inputChan, 2)
	w3 := worker(ctx, cancel, inputChan, 3)
	finalResultChan := fanIn(ctx, w1, w2, w3)

	for result := range finalResultChan {
		fmt.Println(result)
	}

	if ctx.Err() != nil {
		fmt.Println("Прервано ошибкой")
	} else {
		fmt.Println("Нормальное завершение")
	}
}
