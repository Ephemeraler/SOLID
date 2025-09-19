package slurmctl

import (
	"fmt"
	"strings"
	"testing"
)

func TestA(t *testing.T) {
	out := "376352,R,hjxue04,hjxue,828,cn[1856-1860,1869,1970-1978,2041-2048],cp2,normal,None"
	fmt.Println(len(strings.Split(out, ",")))
	// a := true

	// fmt.Printf("%s\n", a)
}
