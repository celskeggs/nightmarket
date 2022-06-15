package main

import (
	"github.com/celskeggs/nightmarket/lib/githelper"
	"github.com/celskeggs/nightmarket/lib/gitremote"
)

func main() {
	gitremote.Mainloop(githelper.Init)
}
