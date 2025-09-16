package slurmctl

import (
	"testing"
)

const samplePartitions = `PartitionName=p1
   AllowGroups=root,group1,group2 AllowAccounts=root,acct1,acct2 AllowQos=ALL
   AllocNodes=ALL Default=NO QoS=N/A
   DefaultTime=NONE DisableRootJobs=NO ExclusiveUser=NO GraceTime=0 Hidden=NO
   MaxNodes=UNLIMITED MaxTime=UNLIMITED MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED
   Nodes=node44
   PriorityJobFactor=1 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO
   OverTimeLimit=NONE PreemptMode=OFF
   State=UP TotalCPUs=36 TotalNodes=1 SelectTypeParameters=NONE
   JobDefaults=(null)
   DefMemPerNode=UNLIMITED MaxMemPerNode=UNLIMITED

PartitionName=p2
   AllowGroups=root,group1,group2,group3 AllowAccounts=root,acct1,acct2 AllowQos=ALL
   AllocNodes=ALL Default=NO QoS=N/A
   DefaultTime=NONE DisableRootJobs=NO ExclusiveUser=NO GraceTime=0 Hidden=NO
   MaxNodes=UNLIMITED MaxTime=UNLIMITED MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED
   Nodes=node2026
   PriorityJobFactor=1 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO
   OverTimeLimit=NONE PreemptMode=OFF
   State=UP TotalCPUs=36 TotalNodes=1 SelectTypeParameters=NONE
   JobDefaults=(null)
   DefMemPerNode=UNLIMITED MaxMemPerNode=UNLIMITED`

func TestParseParttion_MultiplePartitions(t *testing.T) {
	parts := parsePartition(samplePartitions)

	if len(parts) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(parts))
	}

	// First partition assertions
	p1 := parts[0]
	if p1["PartitionName"] != "p1" {
		t.Errorf("p1 PartitionName expected p1, got %q", p1["PartitionName"])
	}
	if p1["Nodes"] != "node44" {
		t.Errorf("p1 Nodes expected node44, got %q", p1["Nodes"])
	}
	if p1["State"] != "UP" {
		t.Errorf("p1 State expected UP, got %q", p1["State"])
	}
	if p1["DefMemPerNode"] != "UNLIMITED" {
		t.Errorf("p1 DefMemPerNode expected UNLIMITED, got %q", p1["DefMemPerNode"])
	}

	// Second partition assertions
	p2 := parts[1]
	if p2["PartitionName"] != "p2" {
		t.Errorf("p2 PartitionName expected p2, got %q", p2["PartitionName"])
	}
	if p2["Nodes"] != "node2026" {
		t.Errorf("p2 Nodes expected node2026, got %q", p2["Nodes"])
	}
	if p2["State"] != "UP" {
		t.Errorf("p2 State expected UP, got %q", p2["State"])
	}
	if p2["MaxTime"] != "UNLIMITED" {
		t.Errorf("p2 MaxTime expected UNLIMITED, got %q", p2["MaxTime"])
	}
}
