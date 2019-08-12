package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
    "github.com/Shopify/sarama"
    kafka "github.com/bwNetFlow/kafkaconnector"
)

func expandDatePart(part string) (string) {
    var expandedPart string
    if len(part) < 2 {
        expandedPart = "0" + part
    } else {
        expandedPart = part
    }
    return expandedPart
}

func createFileName(now time.Time) (name string, ok bool) {
	filename := ""
    year := strconv.Itoa(now.Year())
    month := expandDatePart(strconv.Itoa(int(now.Month())))
    day := expandDatePart(strconv.Itoa(now.Day()))
    hour := expandDatePart(strconv.Itoa(now.Hour()))
    minute := expandDatePart(strconv.Itoa(now.Minute()))
    filename = filename + year + month + day + hour + minute
    filename += ".csv"
	return filename, true
}

func timeout(mins time.Duration, ch chan<- bool) {
	timer := time.NewTimer(mins * time.Minute)
	<-timer.C
	ch <- true
}

var kafkaConn = kafka.Connector{}
var flowCounter, byteCounter uint64

func connectToKafka() {
    broker := "kafka01.bwnf.belwue.de:9093, kafka02.bwnf.belwue.de:9093, kafka03.bwnf.belwue.de:9093, kafka04.bwnf.belwue.de:9093, kafka05.bwnf.belwue.de:9093"
    topic := []string{"flow-messages-anon"}
    consumerGroup := "anon-golang-example"
    kafkaConn.SetAuthAnon()
    kafkaConn.StartConsumer(broker, topic, consumerGroup, sarama.OffsetNewest)
}

func writeToCsv(file *os.File) {
    var flowCounter, byteCounter uint64
    flow := <-kafkaConn.ConsumerChannel()
    flowCounter++
    byteCounter += flow.GetBytes()
    //fmt.Printf("\rflows: %d, bytes: %d GB", flowCounter, byteCounter/1024/1024/1024)
    fmt.Fprintf(file, "%d,%d\n", flowCounter, byteCounter)
}

func main() {
    connectToKafka()
    defer kafkaConn.Close()

	start := time.Now()
	filename, _ := createFileName(start)
	file, err := os.Create(filename)
    if err != nil {
        fmt.Fprintf(os.Stderr, "os.Create: %s: %v\n", filename, err)
        os.Exit(1)
    }
    fmt.Fprintf(os.Stdout, "File %s has been created\n", filename)

	ch := make(chan bool)

	go timeout(1, ch)
	for {
		select {
		case _ = <-ch:
			file.Close()
			start = time.Now()
			filename, _ = createFileName(start)
			file, err = os.Create(filename)
            if err != nil {
                fmt.Fprintf(os.Stderr, "os.Create: %s: %v\n", filename, err)
                os.Exit(1)
            }
            fmt.Fprintf(os.Stdout, "File %s has been created\n", filename)
			go timeout(1, ch)
		default:
            //file.WriteString("bla,blub,brml\n")
            writeToCsv(file)
		}
	}

	file.Close()
}
