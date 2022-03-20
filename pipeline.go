package pipeline

import (
	"errors"
	"fmt"
	"sync"
)

type Pipeline struct {
	in     *Pipe
	lock   sync.RWMutex
	opened bool
}

type Pipe struct {
	handler  func(interface{}) (interface{}, error)
	handlers uint
	retries  uint
	in       chan *input
	out      *Pipe
	stop     chan struct{}
	done     chan struct{}
}

type input struct {
	data  interface{}
	retry uint
}

func (pl *Pipeline) Opened() bool {
	pl.lock.RLock()

	defer pl.lock.RUnlock()

	return pl.opened
}

func (pl *Pipeline) Append(p *Pipe) error {
	if pl.Opened() {
		return errors.New("pipeline already opened")
	}

	if p.handler == nil {
		return errors.New("handler required")
	}

	if p.handlers < 1 {
		return errors.New("handler count required")
	}

	p.in = make(chan *input, p.handlers)
	p.out = nil
	p.done = make(chan struct{}, p.handlers)
	p.stop = make(chan struct{}, p.handlers)

	if pl.in != nil {
		return pl.in.append(p)
	}

	pl.in = p

	return nil
}

func (pl *Pipeline) Open() error {
	if pl.Opened() {
		return errors.New("pipeline already opened")
	}

	if pl.in == nil {
		return errors.New("must append a pipe to the pipeline")
	}

	pl.lock.Lock()

	err := pl.in.open()

	pl.opened = true

	pl.lock.Unlock()

	return err
}

func (pl *Pipeline) Feed(i interface{}) error {
	if !pl.Opened() {
		return errors.New("pipeline not opened")
	}

	if pl.in == nil {
		return errors.New("must append a pipe to the pipeline")
	}

	return pl.in.feed(&input{
		data:  i,
		retry: 0,
	})
}

func (pl *Pipeline) Close() error {
	if !pl.Opened() {
		return errors.New("pipeline not opened")
	}

	if pl.in == nil {
		return errors.New("must append a pipe to the pipeline")
	}

	err := pl.in.close()

	pl.lock.RLock()

	pl.opened = false

	pl.lock.RUnlock()

	return err
}

func (p *Pipe) Handler(h func(interface{}) (interface{}, error)) error {
	if p.handler != nil {
		return errors.New("handler already set")
	}

	p.handler = h

	return nil
}

func (p *Pipe) Handlers(h uint) error {
	if p.handlers != 0 {
		return errors.New("handler count already set")
	}

	p.handlers = h

	return nil
}

func (p *Pipe) Retries(r uint) error {
	if p.retries != 0 {
		return errors.New("retry count already set")
	}

	p.retries = r

	return nil
}

func (p *Pipe) append(end *Pipe) error {
	if p.out != nil {
		return p.out.append(end)
	}

	p.out = end

	return nil
}

func (p *Pipe) open() error {
	if p.out != nil {
		err := p.out.open()

		if err != nil {
			return err
		}
	}

	for h := uint(0); h < p.handlers; h++ {
		go func() {
			for {
				select {
				case in := <-p.in:
					out, err := p.handler(in.data)

					if err != nil {
						in.retry++

						if in.retry <= p.retries {
							p.in <- in
						}

						continue
					}

					if p.out == nil {
						continue
					}

					p.out.in <- &input{
						data:  out,
						retry: 0,
					}
				case <-p.stop:
					if len(p.in) != 0 {
						p.stop <- struct{}{}

						continue
					}

					p.done <- struct{}{}

					return
				}
			}
		}()
	}

	return nil
}

func (p *Pipe) feed(i *input) error {
	p.in <- i

	return nil
}

func (p *Pipe) close() error {
	for i := uint(0); i < p.handlers; i++ {
		p.stop <- struct{}{}
	}

	i := uint(0)

	for range p.done {
		i++

		if i >= p.handlers {
			break
		}
	}

	if i > p.handlers {
		return fmt.Errorf("got %d done, but only expected %d", i, p.handlers)
	}

	close(p.in)

	if p.out != nil {
		return p.out.close()
	}

	return nil
}
