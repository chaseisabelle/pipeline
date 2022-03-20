package pipeline

import (
	"context"
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

		e := p.Handler(func(_ context.Context, i interface{}) (interface{}, error) {
			mux.Lock()

			act++

			mux.Unlock()

			return i, nil
		})

		if e != nil {
			t.Error(e)
		}

		e = p.Handlers(uint(hc))

		if e != nil {
			t.Error(e)
		}

		e = p.Retries(uint(rc))

		if e != nil {
			t.Error(e)
		}

		e = pl.Append(p)

		if e != nil {
			t.Error(e)
		}
	}

	err := pl.Open()

	if err != nil {
		t.Error(err)
	}

	for i := 0; i < max; i++ {
		err = pl.Feed(nil, i)
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

		p.handler = func(_ context.Context, i interface{}) (interface{}, error) {
			mux.Lock()

			defer mux.Unlock()

			act++

			if act <= rc {
				return i, errors.New("poop")
			}

			return i, nil
		}

		p.handlers = uint(hc)
		p.retries = uint(rc)

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
		err = pl.Feed(nil, i)
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

func TestPipeline_Opened(t *testing.T) {
	pl := &Pipeline{}
	p := &Pipe{}

	h := func(_ context.Context, _ interface{}) (interface{}, error) {
		return nil, nil
	}

	err := p.Retries(0)

	if err != nil {
		t.Error(err)
	}

	err = p.Handlers(1)

	if err != nil {
		t.Error(err)
	}

	err = p.Handler(h)

	if err != nil {
		t.Error(err)
	}

	err = pl.Append(p)

	if err != nil {
		t.Error(err)
	}

	if pl.Opened() {
		t.Errorf("pipeline is not opened, but it says it is")
	}

	err = pl.Open()

	if err != nil {
		t.Error(err)
	}

	if !pl.Opened() {
		t.Errorf("pipeline is opened, but it says it is not")
	}

	err = pl.Close()

	if err != nil {
		t.Error(err)
	}

	if pl.Opened() {
		t.Errorf("pipeline is closed, but it says it is opened")
	}
}
