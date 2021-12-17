package main

import (
	"github.com/chaseisabelle/pipeline"
	"strconv"
)

func main() {
	pl := &pipeline.Pipeline{}
	p0 := &pipeline.Pipe{}
	p1 := &pipeline.Pipe{}

	p0.Handler = handler0
	p0.Handlers = 2
	p0.Retries = 0

	p1.Handler = handler1
	p1.Handlers = 2
	p1.Retries = 0

	err := pl.Append(p0)

	if err != nil {
		panic(err)
	}

	err = pl.Append(p1)

	if err != nil {
		panic(err)
	}

	err = pl.Open()

	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		err = pl.Feed(i)
	}

	if err != nil {
		panic(err)
	}

	err = pl.Close()

	if err != nil {
		panic(err)
	}

	println("done")
}

func handler0(i interface{}) (interface{}, error) {
	println("handler0: " + strconv.Itoa(i.(int)))

	return i, nil
}

func handler1(i interface{}) (interface{}, error) {
	println("handler1: " + strconv.Itoa(i.(int)))

	return nil, nil
}
