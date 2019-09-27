package main

import (
	"crypto/sha256"
	"encoding/binary"
	"math/rand"
)

type PseudonymTable []*[]uint16 // this could be uint16, if prefix_length>=16

func NewPseudonymTable(prefix_length uint8, master_seed []byte) PseudonymTable {
	if prefix_length != 16 {
		panic("prefix_length must be 16") // TODO: make Lookup usable with other prefixes
	}

	// create new pseudonym table consisting of nil pointers for each host_table
	var pt []*[]uint16 = make([]*[]uint16, 1<<prefix_length)

	for i := 0; i < len(pt); i++ {
		// create host table and populate it with ordered content
		var host_table []uint16 = make([]uint16, 1<<(32-prefix_length))

		for i := uint32(0); i < 1<<(32-prefix_length); i++ {
			host_table[i] = uint16(i)
		}

		// reseed golang's rng for a new subnet
		reseed_rng(master_seed, i)

		// permutate with fresh randomness
		for i := len(host_table) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			host_table[i], host_table[j] = host_table[j], host_table[i]
		}

		// write host table in larger subnet structure
		pt[i] = &host_table
	}
	return pt
}

func (pt PseudonymTable) Lookup(addr []byte) []byte {
	subnet_part := uint16(addr[0])<<8 + uint16(addr[1])
	host_part := uint16(addr[2])<<8 + uint16(addr[3])

	var host_lookup []uint16 = *pt[subnet_part]
	var pseudonymized_host_part uint16 = host_lookup[host_part]

	return []byte{addr[0], addr[1], byte(pseudonymized_host_part >> 8), byte(pseudonymized_host_part)}
}

func reseed_rng(master []byte, subnet_num int) {
	subnet := make([]byte, 8)
	binary.PutVarint(subnet, int64(subnet_num))

	sha256 := sha256.Sum256(append(master, subnet...))
	var seed int64
	var i uint
	for i = 0; i < 32; i++ {
		seed = seed << i
		seed += int64(sha256[i])
	}
	rand.Seed(seed)
}
