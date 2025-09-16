package slurmctl

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
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

// helper: build fake exec that returns output based on args
func fakeExec(outputFn func(name string, args ...string) string) ExecCommandFunc {
	return func(ctx context.Context, name string, args ...string) *exec.Cmd {
		// Use sh -c to emit prebuilt content
		script := fmt.Sprintf("cat <<'EOF'\n%s\nEOF\n", outputFn(name, args...))
		return exec.CommandContext(ctx, "sh", "-c", script)
	}
}

func TestGetPartitions_AllAndFiltered(t *testing.T) {
	// derive single-partition blocks from samplePartitions
	blocks := strings.Split(samplePartitions, "\n\nPartitionName=")
	if len(blocks) != 2 {
		t.Fatalf("unexpected sample split: %d", len(blocks))
	}
	p1Block := blocks[0]
	p2Block := "PartitionName=" + blocks[1]

	// 1) All partitions: expect both p1 and p2 returned
	scAll := &Slurmctlc{}
	scAll = scAll.WithExecCommand(fakeExec(func(name string, args ...string) string {
		// scontrol show partition
		if len(args) == 2 && args[0] == "show" && args[1] == "partition" {
			return samplePartitions
		}
		return ""
	}))
	partsAll, err := scAll.GetPartitions(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetPartitions(all) error: %v", err)
	}
	if len(partsAll) != 2 {
		t.Fatalf("expected 2 partitions for all, got %d", len(partsAll))
	}
	if partsAll[0]["PartitionName"] != "p1" || partsAll[1]["PartitionName"] != "p2" {
		t.Errorf("unexpected partition order or names: %+v", partsAll)
	}

	// 2) Filtered partitions: only p1
	scOne := &Slurmctlc{}
	scOne = scOne.WithExecCommand(fakeExec(func(name string, args ...string) string {
		// scontrol show partition <name>
		if len(args) == 3 && args[0] == "show" && args[1] == "partition" {
			if args[2] == "p1" {
				return p1Block
			}
			if args[2] == "p2" {
				return p2Block
			}
		}
		return ""
	}))
	partsOne, err := scOne.GetPartitions(context.Background(), []string{"p1"})
	if err != nil {
		t.Fatalf("GetPartitions(p1) error: %v", err)
	}
	if len(partsOne) != 1 {
		t.Fatalf("expected 1 partition for filtered, got %d", len(partsOne))
	}
	if partsOne[0]["PartitionName"] != "p1" {
		t.Errorf("expected PartitionName p1, got %q", partsOne[0]["PartitionName"])
	}
}
