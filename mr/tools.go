package mr

import "log"

func Chk(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
