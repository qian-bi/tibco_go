package main

import (
	"emstest/emsmsg"
	"fmt"
)

func main() {
	buf := make(chan string)
	defer close(buf)
	emsmsg.CreateSession("tcp://localhost:7222", "admin", "", true)
	defer emsmsg.CloseSession()
	fmt.Println("Consumer test......")
	go consumer(buf)
	producer(buf)
	msg := <-buf
	fmt.Printf("Consumer receive: %s\n", msg)
	fmt.Println()

	fmt.Println("Listener test......")
	listener()
	emsmsg.Producer("test.request", true, "<xml><body>test</body>", "MAP", "DATA")
}

func consumer(c chan string) {
	c <- "ready"
	msg := emsmsg.Consumer("test.request", true, 10)
	c <- msg
}

func producer(c chan string) {
	fmt.Printf("Producer %s\n", <-c)
	emsmsg.Producer("test.request", true, "<xml><body>test</body>", "TEXT", "")
}

func listener() {
	fn := func(msg string) {
		fmt.Printf("Listener receive: %s\n", msg)
	}
	emsmsg.Listener("test.request", true, fn)
}
