package main

import (
	"fmt"

	"github.com/wilhasse/innodb-go/btr"
)

func main() {
	fmt.Print(btr.TraceOperations())
}
