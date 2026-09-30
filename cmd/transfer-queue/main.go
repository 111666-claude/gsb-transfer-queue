// Command transfer-queue 跑转服样例。
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"example.com/transferq"
)

// Run 执行一次命令行调用，返回退出码。
func Run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("transfer-queue", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sample := flags.String("sample", "idem", "idem / quota / capacity / scan")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	switch *sample {
	case "idem":
		// 客户端重发同一个请求 ID。
		service := transferq.NewService(map[string]int{"s1": 5})
		first, _ := service.Submit(transferq.Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
		second, status := service.Submit(transferq.Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 10})
		fmt.Fprintf(stdout, "first=%v second=%v status=%s load=%d\n",
			first, second, status, service.Load("s1"))
	case "quota":
		// 同一账号一天转三次。
		service := transferq.NewService(map[string]int{"s1": 9, "s2": 9, "s3": 9})
		results := make([]bool, 0, 3)
		for index, target := range []string{"s1", "s2", "s3"} {
			ok, _ := service.Submit(transferq.Request{
				RequestID: fmt.Sprintf("r%d", index), Account: "a", To: target, AtMs: int64(index),
			})
			results = append(results, ok)
		}
		fmt.Fprintf(stdout, "third=%v\n", results[2])
	case "capacity":
		// 容量只有 1，先占满再转一个。
		service := transferq.NewService(map[string]int{"s1": 1})
		service.Submit(transferq.Request{RequestID: "r1", Account: "a", To: "s1", AtMs: 0})
		ok, status := service.Submit(transferq.Request{RequestID: "r2", Account: "b", To: "s1", AtMs: 10})
		fmt.Fprintf(stdout, "ok=%v status=%s queued=%d load=%d\n",
			ok, status, service.Queued(), service.Load("s1"))
	case "scan":
		// 满服上排三千个请求，再查三千次。
		service := transferq.NewService(map[string]int{"s1": 0})
		for index := 0; index < 3000; index++ {
			service.Submit(transferq.Request{
				RequestID: fmt.Sprintf("r%d", index), Account: "a", To: "s1", AtMs: int64(index),
			})
		}
		for query := 0; query < 3000; query++ {
			service.Queued()
		}
		fmt.Fprintf(stdout, "scanned=%d\n", service.Scanned())
	default:
		fmt.Fprintln(stderr, "需要 --sample idem|quota|capacity|scan")
		return 2
	}
	return 0
}

func main() {
	os.Exit(Run(os.Args[1:], os.Stdout, os.Stderr))
}
