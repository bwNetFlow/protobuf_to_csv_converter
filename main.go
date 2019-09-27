package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"crypto/hmac"
	"crypto/sha256"

	"github.com/Shopify/sarama"
	kafka "github.com/bwNetFlow/kafkaconnector"
)

//var csv_line string
//var csv_line_len int
//var csv_line strings.Builder

var csv_line csvLine

var secret string = "whatever"
var kafkaConn = kafka.Connector{}
var usrcrd = flag.String("u", "", "Path to the user credential file")
var path = flag.String("p", "", "Path to the target directory where the CSV output files are saved")
var duration = flag.Int("d", 5, "Amount of time that is written into one file")

/* --- PROFILING CODE --- */
var memprofile = flag.String("memprofile", "", "Write memory profile to file")
var cpuprofile = flag.String("cpuprofile", "", "Write cpu profile to file")

type csvLine struct {
	bytes         uint64
	packets       uint64
	srcAddr       net.IP
	dstAddr       net.IP
	srcPort       uint32
	dstPort       uint32
	timeFlowStart uint64
	duration      uint64
}

func (line *csvLine) writeLine(file *os.File) {
	fmt.Fprintf(file, "%v,%v,%v,%v,%v,%v,%v,%v\n", line.bytes, line.packets, line.srcAddr, line.dstAddr, line.srcPort, line.dstPort, line.timeFlowStart, line.duration)
}

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
		tmp = strings.Split(line, "=")
		content[strings.Trim(tmp[0], " ")] = strings.Trim(tmp[1], " ")
	}
	ok = true
	return content, ok
}

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

func connectToKafka(credentials map[string]string) {
	broker := credentials["brokers"]
	topic := []string{credentials["topic"]}
	consumerGroup := credentials["grp_id"]
	kafkaConn.SetAuth(credentials["user"], credentials["pwd"])
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

func writeToCsv(credentials map[string]string, file *os.File, fields []string, pt PseudonymTable) {
	var csv_line string
	var csv_line_len int

	//var csv_line strings.Builder

	flow, ok := <-kafkaConn.ConsumerChannel()
	if !ok {
		fmt.Fprintf(os.Stderr, "Could not read from consumer channel... skipping message.")
		return
	}
	reflected_flow := reflect.ValueOf(flow)
	var addr net.IP
	for _, fieldname := range fields {
		field := reflect.Indirect(reflected_flow).FieldByName(fieldname)
		if fieldname == "Duration" {
			flowStart := reflect.Indirect(reflected_flow).FieldByName("TimeFlowStart").Interface().(uint64)
			flowEnd := reflect.Indirect(reflected_flow).FieldByName("TimeFlowEnd").Interface().(uint64)
			Duration := flowEnd - flowStart
			csv_line = csv_line + fmt.Sprint(Duration) + ","
			//fmt.Fprintf(&csv_line, "%v,", Duration)
			//csv_line.WriteString(fmt.Sprint(Duration))
		} else if field.Kind() == reflect.Slice && reflect.ValueOf(field.Bytes()[0]).Kind() == reflect.Uint8 {
			byteAddr := field.Bytes()
			if credentials["anonymization"] == "yes" && (fieldname == "SrcAddr" || fieldname == "DstAddr") {
				if len(byteAddr) == 4 {
					byteAddr = pt.Lookup(byteAddr)
					/*
						h.Write(byteAddr[2:])
						byteAddr[2] = h.Sum(nil)[2]
						byteAddr[3] = h.Sum(nil)[3]
					*/
				} else if len(byteAddr) == 16 {
					h := hmac.New(sha256.New, []byte(secret))
					h.Write(byteAddr[8:])
					for i := 8; i < 16; i++ {
						byteAddr[i] = h.Sum(nil)[i]
					}
				}
			}
			addr = net.IP(byteAddr)
			csv_line = csv_line + fmt.Sprint(addr) + ","
			//fmt.Fprintf(&csv_line, "%v,", addr)
			//csv_line.WriteString(fmt.Sprint(addr))
		} else {
			csv_line = csv_line + fmt.Sprint(field) + ","
			//fmt.Fprintf(&csv_line, "%v,", field)
			//csv_line.WriteString(fmt.Sprint(field))
		}
	}
	csv_line_len = len(csv_line)
	csv_line = csv_line[:csv_line_len-1]
	//fmt.Fprintf(file, "%s\n", csv_line.String()[:csv_line.Len()-1])
	fmt.Fprintf(file, "%s\n", csv_line)
	csv_line = ""
}

func main() {
	flag.Parse()
	fields := flag.Args()

	credentials, _ := readIni(*usrcrd)

	/* --- Initialize Knuth-Fisher-Yates Tables  --- */
	var pt PseudonymTable
	if credentials["anonymization"] == "yes" {
		fmt.Fprintf(os.Stdout, "Pseudonymization YES... initializing tables...\n")
		pt = NewPseudonymTable(16, []byte("masterkey"))
	} else {
		fmt.Fprintf(os.Stdout, "Pseudonymization NO\n")
	}

	connectToKafka(credentials)
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
			err := file.Close()
			if err != nil {
				fmt.Println(err)
			}

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
			writeToCsv(credentials, file, fields, pt)
		}
	}
}
