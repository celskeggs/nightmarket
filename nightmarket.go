package main

import (
	"fmt"
	"os"
	"path"

	"github.com/celskeggs/nightmarket/lib/annexhelper"
	"github.com/celskeggs/nightmarket/lib/annexremote"
	"github.com/celskeggs/nightmarket/lib/githelper"
	"github.com/celskeggs/nightmarket/lib/gitremote"
	"github.com/celskeggs/nightmarket/lib/nmcmd"
)

func main() {
	switch path.Base(os.Args[0]) {
	case "git-annex-remote-nightmarket":
		annexremote.Mainloop(annexhelper.Init())
	case "git-remote-nightmarket":
		gitremote.Mainloop(githelper.Init)
	case "nightmarket":
		nmcmd.Main()
	default:
		_, _ = fmt.Fprintf(os.Stderr, "nightmarket: unrecognized argv[0]: %q", os.Args[0])
		os.Exit(1)
	}
}
