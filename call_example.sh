#!/bin/sh
./main -u ~/tmp/test.ini -p /vol/raid/datasets/bwNetFlow/ -d 60 -gc 50 -t "Oct 9, 2019 at 8:00pm (CEST)" TimeFlowStart TimeFlowEnd Duration Bytes Packets SrcAddr DstAddr Etype Proto SrcPort DstPort SrcIf DstIf SrcVlan DstVlan VlanId IngressVrfID EgressVrfID IPTos ForwardingStatus IPTTL TCPFlags IcmpType IcmpCode IPv6FlowLabel IPv6ExtensionHeaders FragmentId FragmentOffset BiFlowDirection SrcAS DstAS NextHop NextHopAS SrcNet DstNet SrcIf DstIf FlowDirection Cid
