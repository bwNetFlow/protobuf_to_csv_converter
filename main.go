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
	"encoding/binary"
	"math"
	"math/rand"

	"github.com/Shopify/sarama"
	kafka "github.com/bwNetFlow/kafkaconnector"

	"runtime/debug"
)

var secret string = "whatever"
var kafkaConn = kafka.Connector{}

/* --- COMMAND LINE FLAGS --- */
var usrcrd = flag.String("u", "", "Path to the user credential file")
var path = flag.String("p", "", "Path to the target directory where the CSV output files are saved")
var duration = flag.Int("d", 5, "Amount of time that is written into one file")
var gc = flag.Int("gc", 50, "Garbage Collector behavior. Default=50%")

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

/* --- TODO: ERROR HANDLING IN THE CASE THAT THE RANGE BIT ARE SMALLER THAN 8 --- */
func init_subnets(subnet_range_bit int, host_range_bit int, master_key []byte) *[]*[]uint16 {
	subnet_byte_repr := make([]byte, 8)
	subnet_list_length := int(math.Pow(2, float64(subnet_range_bit)))
	host_list_length := int(math.Pow(2, float64(host_range_bit)))
	fmt.Fprintf(os.Stdout, "subnet: %d, host: %d\n", subnet_list_length, host_list_length)
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

func processIPs(credentials map[string]string, addr []byte, lists *[]*[]uint16) net.IP {
	if credentials["anonymization"] == "yes" {
		if len(addr) == 4 {
			addr = pseudomyze_ip(addr, lists)
		} else if len(addr) == 16 {
			h := hmac.New(sha256.New, []byte(secret))
			h.Write(addr[8:])
			for i := 8; i < 16; i++ {
				addr[i] = h.Sum(nil)[i]
			}
		}
	}
	return net.IP(addr)
}

func writeToCsv(credentials map[string]string, file *os.File, fields []string, lists *[]*[]uint16) {
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
					h := hmac.New(sha256.New, []byte(secret))
					h.Write(byteAddr[8:])
					for i := 8; i < 16; i++ {
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
	csv_line_len = len(csv_line)
	csv_line = csv_line[:csv_line_len-1]
	fmt.Fprintf(file, "%s\n", csv_line)
	csv_line = ""
}

func main() {
	flag.Parse()
	fields := flag.Args()

	/* --- Set GC Percentage  --- */
	debug.SetGCPercent(*gc)

	/* --- Process given credential file  --- */
	credentials, _ := readIni(*usrcrd)

	/* --- Initialize Knuth-Fisher-Yates Tables  --- */
	var lists *[]*[]uint16
	if credentials["anonymization"] == "yes" {
		fmt.Fprintf(os.Stdout, "Pseudonymization YES... initializing tables...\n")
		lists = init_subnets(16, 16, []byte("masterkey"))
	} else {
		lists = nil
		fmt.Fprintf(os.Stdout, "Pseudonymization NO\n")
	}

	/* --- Connect to the Kafka Cluster --- */
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
			writeToCsv(credentials, file, fields, lists)
		}
	}

	file.Close()
}
