package main

import "fmt"

func fmtKB(bytes float64) string {
	return fmt.Sprintf("%.1fKB", bytes/1024)
}
