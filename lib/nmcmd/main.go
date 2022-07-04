package nmcmd

import (
	"fmt"
	"os"
)

func Main() {
	if len(os.Args) == 3 && os.Args[1] == "init" {
		err := initRepo(os.Args[2])
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s init: %v\n", os.Args[0], err)
			os.Exit(1)
		}
	} else if len(os.Args) == 2 && os.Args[1] == "repair" {
		err := repairRepo()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s repair: %v\n", os.Args[0], err)
			os.Exit(1)
		}
	} else {
		_, _ = fmt.Fprintf(os.Stderr, "usage: %s init <annex-directory>\n", os.Args[0])
		_, _ = fmt.Fprintf(os.Stderr, "usage: %s repair\n", os.Args[0])
		os.Exit(1)
	}
}
