package flowfilter

import (
	"fmt"
	"github.com/bwNetFlow/ip_prefix_trie"
	flow "github.com/bwNetFlow/protobuf/go"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
)

func NewFlowFilter(cids string, ipv4s string, ipv6s string, peers string) *FlowFilter {

	var validCustomerIDs []int
	var validPeers []string

	// We have to use separate tries for IPv4 and IPv6
	var validIPTrieV4, validIPTrieV6 ip_prefix_trie.TrieNode
	var ipFilterSet bool

	// customer ID
	if cids != "" {
		stringIDs := strings.Split(cids, ",")
		for _, stringID := range stringIDs {
			customerID, err := strconv.Atoi(stringID)
			if err != nil {
				continue
			}
			validCustomerIDs = append(validCustomerIDs, customerID)
		}
		sort.Ints(validCustomerIDs)

		outputStr := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(validCustomerIDs)), ","), "[]")
		log.Printf("Filter flows for customer ids %s\n", outputStr)
	} else {
		log.Printf("No customer filter enabled.\n")
	}

	// IPs v4
	if ipv4s != "" {
		ipFilterSet = true
		stringIDs := strings.Split(ipv4s, ",")
		validIPTrieV4.Insert(true, stringIDs)
		// validIPTrieV4.Print("", true)
		log.Printf("Filter flows for IPs v4: %s\n", ipv4s)
	} else {
		log.Printf("No IP v4 filter enabled.\n")
	}

	// IPs v6
	if ipv6s != "" {
		ipFilterSet = true
		stringIDs := strings.Split(ipv6s, ",")
		validIPTrieV6.Insert(true, stringIDs)
		log.Printf("Filter flows for IPs v6: %s\n", ipv6s)
	} else {
		log.Printf("No IP v6 filter enabled.\n")
	}

	// peers
	if peers != "" {
		stringIDs := strings.Split(peers, ",")
		for _, stringID := range stringIDs {
			peer := strings.Trim(stringID, " ")
			validPeers = append(validPeers, peer)
		}
		sort.Strings(validPeers)

		// outputStr := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(validPeers)), ","), "[]")
		log.Printf("Filter flows for peers %v\n", validPeers)
	} else {
		log.Printf("No peer filter enabled.\n")
	}

	return &FlowFilter{
		validCustomerIDs: validCustomerIDs,
		validPeers:       validPeers,
		validIPTrieV4:    validIPTrieV4,
		validIPTrieV6:    validIPTrieV6,
		ipFilterSet:      ipFilterSet,
	}
}

type FlowFilter struct {
	validCustomerIDs []int
	validPeers       []string
	validIPTrieV4    ip_prefix_trie.TrieNode
	validIPTrieV6    ip_prefix_trie.TrieNode
	ipFilterSet      bool
}

func (flowFilter *FlowFilter) FilterApplies(flow *flow.FlowMessage) bool {
	// customerID filter
	if len(flowFilter.validCustomerIDs) == 0 || flowFilter.isValidCustomerID(int(flow.GetCid())) {
		// IP subnet filter
		if !flowFilter.ipFilterSet || flowFilter.isValidIP(flow.GetSrcAddr()) || flowFilter.isValidIP(flow.GetDstAddr()) {
			// peer filter
			if len(flowFilter.validPeers) == 0 || flowFilter.isValidPeer(flow.GetSrcIfDesc()) || flowFilter.isValidPeer(flow.GetDstIfDesc()) {
				return true
			}
		}
	}
	return false
}

func (flowFilter *FlowFilter) isValidCustomerID(cid int) bool {
	pos := sort.SearchInts(flowFilter.validCustomerIDs, cid)
	if pos == len(flowFilter.validCustomerIDs) {
		return false
	}
	return flowFilter.validCustomerIDs[pos] == cid
}

func (flowFilter *FlowFilter) isValidIP(IP []byte) bool {
	// TODO improve the following workarounds ...
	test := net.IP(IP)
	var prefix string
	if test.To4() == nil {
		// ipv6
		prefix = "64"
	} else {
		// ipv4
		prefix = "32"
	}
	ipAddrStr := net.IP(IP).String() + "/" + prefix
	ipAddr, _, _ := net.ParseCIDR(ipAddrStr)
	foundIP := false
	if ipAddr.To4() == nil {
		foundIP, _ = flowFilter.validIPTrieV6.Lookup(ipAddr).(bool)
	} else {
		foundIP, _ = flowFilter.validIPTrieV4.Lookup(ipAddr).(bool)
	}
	return foundIP
}

func (flowFilter *FlowFilter) isValidPeer(peer string) bool {
	pos := sort.SearchStrings(flowFilter.validPeers, peer)
	if pos == len(flowFilter.validPeers) {
		return false
	}
	return flowFilter.validPeers[pos] == peer
}
