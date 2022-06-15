package main

import (
	"github.com/celskeggs/nightmarket/lib/annexhelper"
	"github.com/celskeggs/nightmarket/lib/annexremote"
)

func main() {
	annexremote.Mainloop(annexhelper.Init())
}
