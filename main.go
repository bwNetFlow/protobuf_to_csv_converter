/* --- TODO: Better Naming (lists, functions) --- */

package main

import (
	"container/list"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"math/rand"

	"github.com/Shopify/sarama"
	kafka "github.com/bwNetFlow/kafkaconnector"

	"runtime/debug"
)

const longForm = "Jan 2, 2006 at 3:04pm (MST)"

var b_v4_Masterkey []byte
var b_v6_Masterkey []byte
var kafkaConn = kafka.Connector{}

/* --- COMMAND LINE FLAGS --- */
var usrcrd = flag.String("u", "", "Path to the user credential file")
var path = flag.String("p", "", "Path to the target directory where the CSV output files are saved")
var duration = flag.Int("d", 5, "Amount of time that is written into one file")
var gc = flag.Int("gc", 50, "Defines garbage collector behavior. Default=50%")
var timeToStart = flag.String("t", "", "Time when the collecting should start at")
var finalPath = ""

type fileNode struct {
	uts uint64
	fd  *os.File
}

func readIni(path string) (content map[string]string, ok bool) {
	log.Printf("Trying to read from file %s\n", path)
	ini, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("ioutil.ReadFile: Error reading %s (%v)\n", path, err)
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

func retrieve_masterkeys(credentials map[string]string) (b_v4_masterkey []byte, b_v6_masterkey []byte) {
	s_v4_Masterkey := credentials["v4_masterkey"]
	s_v6_Masterkey := credentials["v6_masterkey"]
	if s_v4_Masterkey == "" || s_v6_Masterkey == "" {
		return nil, nil
	} else {
		return []byte(s_v4_Masterkey), []byte(s_v6_Masterkey)
	}
}

/* --- TODO: ERROR HANDLING IN THE CASE THAT THE RANGE BIT ARE SMALLER THAN 8 --- */
func init_subnets(subnet_range_bit int, host_range_bit int, master_key []byte) *[]*[]uint16 {
	subnet_byte_repr := make([]byte, 8)
	subnet_list_length := int(math.Pow(2, float64(subnet_range_bit)))
	host_list_length := int(math.Pow(2, float64(host_range_bit)))
	subnet_list := make([]*[]uint16, subnet_list_length)
	for i := 0; i < subnet_list_length; i++ {
		binary.PutVarint(subnet_byte_repr, int64(i))
		seed := calc_key(master_key, subnet_byte_repr)
		tmp := make([]uint16, host_list_length)
		subnet_list[i] = &tmp
		init_list(subnet_list[i], host_list_length)
		shuffle_list(subnet_list[i], seed)
	}

	return &subnet_list
}

func calc_key(master []byte, subnet []byte) int64 {
	sha256 := sha256.Sum256(append(master, subnet...))
	var key int64
	var i uint
	for i = 0; i < 32; i++ {
		key = key << i
		key += int64(sha256[i])
	}
	return key
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

func createFilePath() {
	finalPath = strings.TrimSuffix(*path, "/") + "/" + strings.Replace(*timeToStart, " ", "", -1) + "/"
	os.Mkdir(finalPath, 0770)
}

func createFileNode(time uint64, timeOffset uint64, fields []string) fileNode {
	var node fileNode
	node.uts = time - timeOffset
	pathname := finalPath + strconv.FormatUint(node.uts, 10) + ".csv"

	file, err := os.Create(pathname)
	if err != nil {
		log.Printf("os.Create: %s: %v\n", pathname, err)
		os.Exit(1)
	}
	log.Printf("File %s has been created\n", pathname)

	ok := writeColumnDescr(file, fields)
	if !ok {
		log.Printf("No descriptors defined... exiting...\n")
		os.Exit(1)
	}

	node.fd = file

	return node
}

func createFileList(fields []string) *list.List {
	l := list.New()
	time := uint64(time.Now().Unix())

	for i := uint64(0); i < 4*uint64(*duration)*60; i = i + uint64(*duration)*60 {
		node := createFileNode(time, i, fields)
		l.PushBack(node)
	}
	return l
}

func updateFileList(l *list.List, fields []string) {
	time := uint64(time.Now().Unix())

	node := createFileNode(time, 0, fields)
	l.PushFront(node)
	tmp := l.Back().Value.(fileNode)
	tmp.fd.Close()
	l.Remove(l.Back())
}

func closeFileList(l *list.List) bool {
	var err error
	for e := l.Front(); e != nil; e = e.Next() {
		err = e.Value.(fileNode).fd.Close()
	}
	if err != nil {
		return false
	} else {
		return true
	}
}

func init_list(list *[]uint16, different_nmbrs int) {
	for i := 0; i < different_nmbrs; i++ {
		(*list)[i] = uint16(i)
	}
}

func shuffle_list(list *[]uint16, seed int64) {
	rand.Seed(seed)
	for i := len(*list) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		(*list)[i], (*list)[j] = (*list)[j], (*list)[i]
	}
}

func pseudomyze_ip(addr []byte, lists *[]*[]uint16) []byte {
	var subnet_part uint16
	var host_part uint16

	subnet_part = uint16(addr[0])
	subnet_part = subnet_part << 8
	subnet_part += uint16(addr[1])

	host_part = uint16(addr[2])
	host_part = host_part << 8
	host_part += uint16(addr[3])

	pseudonymized_host_part := (*(*lists)[subnet_part])[host_part]

	pseudonymized_byte_addr := addr
	pseudonymized_byte_addr[2] = byte(pseudonymized_host_part >> 8)
	pseudonymized_byte_addr[3] = byte(pseudonymized_host_part)
	return pseudonymized_byte_addr
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

func writeCsvLine(fileList *list.List, csv_line *string, flowTimeStamp uint64) {
	for e := fileList.Front(); e != nil; e = e.Next() {
		if e.Value.(fileNode).uts < flowTimeStamp {
			fmt.Fprintf(e.Value.(fileNode).fd, "%s\n", *csv_line)
			return
		}
	}
}

func writeToCsv(credentials map[string]string, fileList *list.List, fields []string, lists *[]*[]uint16) {
	var csv_line string
	var csv_line_len int

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
		} else if field.Kind() == reflect.Slice && reflect.ValueOf(field.Bytes()[0]).Kind() == reflect.Uint8 {
			byteAddr := field.Bytes()
			if credentials["anonymization"] == "yes" && (fieldname == "SrcAddr" || fieldname == "DstAddr") {
				if len(byteAddr) == 4 {
					byteAddr = pseudomyze_ip(byteAddr, lists)
				} else if len(byteAddr) == 16 {
					h := hmac.New(sha256.New, b_v6_Masterkey)
					h.Write(byteAddr[6:])
					for i := 6; i < 16; i++ {
						byteAddr[i] = h.Sum(nil)[i]
					}
				}
			}
			addr = net.IP(byteAddr)
			csv_line = csv_line + fmt.Sprint(addr) + ","
		} else {
			csv_line = csv_line + fmt.Sprint(field) + ","
		}
	}
	flowTimeStamp := flow.GetTimeFlowStart()
	csv_line_len = len(csv_line)
	csv_line = csv_line[:csv_line_len-1]
	writeCsvLine(fileList, &csv_line, flowTimeStamp)
}

func wait(t time.Time) {
	currentTime := time.Now()
	if t.Before(currentTime) {
		return
	}
	currentTimeUnix := currentTime.Unix()
	tUnix := t.Unix()
	timeDiffUnix := tUnix - currentTimeUnix
	log.Printf("Going to wait %v seconds...\n", timeDiffUnix)
	time.Sleep(time.Duration(timeDiffUnix) * time.Second)
	return
}

func main() {
	flag.Parse()
	fields := flag.Args()

	/* --- Set GC Percentage  --- */
	debug.SetGCPercent(*gc)

	/* --- Process given credential file  --- */
	credentials, _ := readIni(*usrcrd)

	/* --- Wait --- */
	t, _ := time.Parse(longForm, *timeToStart)
	log.Println("Collecting starts at:", t)
	wait(t)

	/* --- Initialize Knuth-Fisher-Yates Tables  --- */
	var lists *[]*[]uint16
	if credentials["anonymization"] == "yes" {
		log.Printf("Pseudonymization YES... initializing tables...\n")
		/* --- Retrieve Masterkey --- */
		b_v4_Masterkey, b_v6_Masterkey = retrieve_masterkeys(credentials)
		if b_v4_Masterkey == nil {
			log.Printf("No Masterkey given... exiting...\n")
			os.Exit(255)
		}
		lists = init_subnets(16, 16, b_v4_Masterkey)
	} else {
		lists = nil
		log.Printf("Pseudonymization NO\n")
	}

	createFilePath()

	/* --- Connect to the Kafka Cluster --- */
	connectToKafka(credentials)
	defer kafkaConn.Close()

	fileList := createFileList(fields)

	ch := make(chan bool)

	go timeout(time.Duration(*duration), ch)

	for {
		select {
		case _ = <-ch:
			updateFileList(fileList, fields)

			go timeout(time.Duration(*duration), ch)
		default:
			writeToCsv(credentials, fileList, fields, lists)
		}
	}

	closeFileList(fileList)
}
