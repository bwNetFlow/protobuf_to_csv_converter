package main

import (
	"bytes"
	"testing"
)

func TestNewPseudonymTable(t *testing.T) {
	pt := NewPseudonymTable(16, []byte("secret"))
	ip := []byte{0, 0, 110, 1}
	pseudo := pt.Lookup(ip)
	if bytes.Equal(pseudo, ip) || pseudo[2]+pseudo[3] == 0 {
		t.Errorf("Not properly anonymized: %v became %v", ip, pseudo)
	}
}
