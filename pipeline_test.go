package pipeline

import (
	"errors"
	"sync"
	"testing"
)

func TestPipeline(t *testing.T) {
	pc := 5
	hc := 2
	rc := 0

	max := 10000
	exp := max * pc * (rc + 1)
	act := 0
	mux := sync.Mutex{}

	pl := &Pipeline{}

	for i := 0; i < pc; i++ {
		p := &Pipe{}

		p.Handler = func(i interface{}) (interface{}, error) {
			mux.Lock()
			act++
			mux.Unlock()

			return i, nil
		}

		p.Handlers = uint(hc)
		p.Retries = uint(rc)

		err := pl.Append(p)

		if err != nil {
			t.Error(err)
		}
	}

	err := pl.Open()

	if err != nil {
		t.Error(err)
	}

	for i := 0; i < max; i++ {
		err = pl.Feed(i)
	}

	if err != nil {
		t.Error(err)
	}

	err = pl.Close()

	if err != nil {
		t.Error(err)
	}

	if act != exp {
		t.Errorf("expected %d, actual %d", exp, act)
	}
}

func TestPipeline_Retries(t *testing.T) {
	pc := 1
	hc := 1
	rc := 1

	max := 1
	exp := max * pc * (rc + 1)
	act := 0
	mux := sync.Mutex{}

	pl := &Pipeline{}

	for i := 0; i < pc; i++ {
		p := &Pipe{}

		p.Handler = func(i interface{}) (interface{}, error) {
			mux.Lock()

			defer mux.Unlock()

			act++

			if act <= rc {
				return i, errors.New("poop")
			}

			return i, nil
		}

		p.Handlers = uint(hc)
		p.Retries = uint(rc)

		err := pl.Append(p)

		if err != nil {
			t.Error(err)
		}
	}

	err := pl.Open()

	if err != nil {
		t.Error(err)
	}

	for i := 0; i < max; i++ {
		err = pl.Feed(i)
	}

	if err != nil {
		t.Error(err)
	}

	err = pl.Close()

	if err != nil {
		t.Error(err)
	}

	if act != exp {
		t.Errorf("expected %d, actual %d", exp, act)
	}
}
