package pipeline

import (
	"errors"
	"fmt"
)

type Pipeline struct {
	in *Pipe
}

type Pipe struct {
	Handler  func(interface{}) (interface{}, error)
	Handlers uint
	Retries  uint
	in       chan *input
	out      *Pipe
	stop     chan struct{}
	done     chan struct{}
}

type input struct {
	data  interface{}
	retry uint
}

func (pl *Pipeline) Append(p *Pipe) error {
	if p.Handler == nil {
		return errors.New("handler required")
	}

	if p.Handlers < 1 {
		return errors.New("handler count required")
	}

	p.in = make(chan *input, p.Handlers)
	p.out = nil
	p.done = make(chan struct{}, p.Handlers)
	p.stop = make(chan struct{}, p.Handlers)

	if pl.in != nil {
		return pl.in.append(p)
	}

	pl.in = p

	return nil
}

func (pl *Pipeline) Open() error {
	if pl.in != nil {
		return pl.in.open()
	}

	return errors.New("must append a pipe to the pipeline")
}

func (pl *Pipeline) Feed(i interface{}) error {
	if pl.in == nil {
		return errors.New("must append a pipe to the pipeline")
	}

	return pl.in.feed(&input{
		data:  i,
		retry: 0,
	})
}

func (pl *Pipeline) Close() error {
	if pl.in == nil {
		return errors.New("must append a pipe to the pipeline")
	}

	return pl.in.close()
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

	for h := uint(0); h < p.Handlers; h++ {
		go func() {
			for {
				select {
				case in := <-p.in:
					out, err := p.Handler(in.data)

					if err != nil {
						in.retry++

						if in.retry <= p.Retries {
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
	for i := uint(0); i < p.Handlers; i++ {
		p.stop <- struct{}{}
	}

	i := uint(0)

	for range p.done {
		i++

		if i >= p.Handlers {
			break
		}
	}

	if i > p.Handlers {
		return fmt.Errorf("got %d done, but only expected %d", i, p.Handlers)
	}

	close(p.in)

	if p.out != nil {
		return p.out.close()
	}

	return nil
}
