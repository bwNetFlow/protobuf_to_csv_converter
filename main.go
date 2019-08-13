package main

import (
	"flag"
	"fmt"
	"os"
	"reader"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	kafka "github.com/bwNetFlow/kafkaconnector"
)

var kafkaConn = kafka.Connector{}
var usrcrd = flag.String("u", "", "Path for user credential file")
var path = flag.String("p", "", "Path to CSV file that is created")
var duration = flag.Int("d", 5, "Amount of time that is written into one file")

func expandDatePart(part string) string {
	var expandedPart string
	if len(part) < 2 {
		expandedPart = "0" + part
	} else {
		expandedPart = part
	}
	return expandedPart
}

func createFileName(path string, now time.Time) (name string, ok bool) {
	filename := ""
	year := strconv.Itoa(now.Year())
	month := expandDatePart(strconv.Itoa(int(now.Month())))
	day := expandDatePart(strconv.Itoa(now.Day()))
	hour := expandDatePart(strconv.Itoa(now.Hour()))
	minute := expandDatePart(strconv.Itoa(now.Minute()))
	filename = path + year + month + day + hour + minute
	filename += ".csv"
	return filename, true
}

func timeout(mins time.Duration, ch chan<- bool) {
	timer := time.NewTimer(mins * time.Minute)
	<-timer.C
	ch <- true
}

func connectToKafka() {
	broker := "kafka01.bwnf.belwue.de:9093, kafka02.bwnf.belwue.de:9093, kafka03.bwnf.belwue.de:9093, kafka04.bwnf.belwue.de:9093, kafka05.bwnf.belwue.de:9093"
	topic := []string{"flow-messages-anon"}
	consumerGroup := "anon-golang-example"
	kafkaConn.SetAuthAnon()
	kafkaConn.StartConsumer(broker, topic, consumerGroup, sarama.OffsetNewest)
}

func writeColumnDescr(file *os.File, fields []string) bool {
	if len(fields) == 0 {
		return false
	} else {
		descr := strings.Join(fields, ",")
		fmt.Fprintf(file, "%s\n", descr)
		return true
	}
}

func writeToCsv(file *os.File, fields []string) {
	var csv_line string
	var csv_line_len int
	flow, ok := <-kafkaConn.ConsumerChannel()
	if !ok {
		fmt.Fprintf(os.Stderr, "Could not read from consumer channel... skipping message.")
		return
	}
	reflected_flow := reflect.ValueOf(flow)
	for _, fieldname := range fields {
		field := reflect.Indirect(reflected_flow).FieldByName(fieldname)
		csv_line = csv_line + fmt.Sprint(field) + ","
	}
	csv_line_len = len(csv_line)
	csv_line = csv_line[:csv_line_len-1]
	fmt.Fprintf(file, "%s\n", csv_line)
}

func main() {
	flag.Parse()
	fields := flag.Args()

	credentials, _ := reader.readIni(*usrcrd)
	for k, v := range credentials {
		fmt.Fprintf("%s:%s\n", k, v)
	}

	connectToKafka()
	defer kafkaConn.Close()

	start := time.Now()
	filename, _ := createFileName(*path, start)
	file, err := os.Create(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "os.Create: %s: %v\n", filename, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "File %s has been created\n", filename)
	ok := writeColumnDescr(file, fields)
	if !ok {
		fmt.Fprintf(os.Stderr, "No descriptors defined... exiting...\n")
		os.Exit(1)
	}

	ch := make(chan bool)

	go timeout(time.Duration(*duration), ch)
	for {
		select {
		case _ = <-ch:
			file.Close()
			start = time.Now()
			filename, _ = createFileName(*path, start)
			file, err = os.Create(filename)
			if err != nil {
				fmt.Fprintf(os.Stderr, "os.Create: %s: %v\n", filename, err)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stdout, "File %s has been created\n", filename)
			ok := writeColumnDescr(file, fields)
			if !ok {
				fmt.Fprintf(os.Stderr, "No descriptors defined... exiting...\n")
				os.Exit(1)
			}
			go timeout(time.Duration(*duration), ch)
		default:
			writeToCsv(file, fields)
		}
	}

	file.Close()
}
