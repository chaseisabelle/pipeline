# pipeline
_a primitive framework for pipeline architecture in golang_

---

a very simple example:

```go
package main

import (
    ...
    "github.com/chaseisabelle/pipeline"
    ...
)

func main() {
	...

	pl := pipeline.Pipeline{}

	...

	p := &pipeline.Pipe{
		Handler:  handler,
		Handlers: 10,
		Retries:  0,
	}

	...

	err := pl.Append(p)

	...

	err = pl.Open()

	...

	err = pl.Feed("hello world")

	...

	err = pl.Close()

	...
}

func handler(i interface{}) (interface{}, error) {
	str, ok := i.(string)

	if !ok {
		return nil, fmt.Errorf("invalid handler input %+v", i)
	}
	
	fmt.Printf("%s\n", str)
	
	return str, nil
}
```