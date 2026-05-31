package main

import (
	"fmt"
	"sync"
	"time"
)

func generator(nums ...int) <-chan int {
	out := make(chan int)

	go func() {
		for _, n := range nums {
			out <- n
		}
		close(out)
	}()

	return out
}

func worker(in <-chan int, workerId int) <-chan int {
	out := make(chan int)

	go func() {
		for n := range in {
			time.Sleep(300 * time.Millisecond)
			fmt.Printf("[Воркер %d] Обрабатывает число: %d\r\n", workerId, n)
			out <- n * n
		}
		close(out)
	}()
	return out
}

func fanIn(channels ...<-chan int) <-chan int {
	var wg sync.WaitGroup

	multiplexedStream := make(chan int)
	multiplex := func(c <-chan int) {
		defer wg.Done()
		for n := range c {
			multiplexedStream <- n
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
	inputChan := generator(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)

	w1 := worker(inputChan, 1)
	w2 := worker(inputChan, 2)
	w3 := worker(inputChan, 3)
	finalResultChan := fanIn(w1, w2, w3)

	for result := range finalResultChan {
		fmt.Println(result)

	}
}
