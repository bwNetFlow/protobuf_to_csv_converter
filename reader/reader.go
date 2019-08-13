package reader

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

//var path = flag.String("p", "", "Path to file with user credentials")

func readIni(path string) (content map[string]string, ok bool) {
	fmt.Fprintf(os.Stdout, "Trying to read from file %s\n", path)
	ini, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ioutil.ReadFile: Error reading %s (%v)\n", path, err)
		ok = false
		return content, ok
	}
    content = make(map[string]string)
	var tmp []string
	for _, line := range strings.Split(string(ini), "\n") {
		if len(line) == 0 {
			continue
		}
		tmp = strings.Split(line, ":")
		content[strings.Trim(tmp[0], " ")] = strings.Trim(tmp[1], " ")
	}
	ok = true
	return content, ok
}

/*
func main() {
	flag.Parse()
	content, _  := readIni(*path)
	for k, v := range content {
		fmt.Fprintf(os.Stdout, "KEY: %s with VALUE: %s\n", k, v)
	}
}
*/
