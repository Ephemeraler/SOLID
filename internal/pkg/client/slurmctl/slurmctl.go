package slurmctl

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"solid/internal/pkg/client/slurmctl/models"
	"strconv"
	"strings"
)

// ExecCommandFunc 定义 exec.CommandContext 的函数签名，方便 mock 测试.
type ExecCommandFunc func(ctx context.Context, name string, args ...string) *exec.Cmd

// Slurmctlc 提供使用命令与 slurmctld 交互的功能.
type Slurmctlc struct {
	execCommand ExecCommandFunc
	logger      *slog.Logger
}

// WithExecCommand 设置 exec.CommandContext 的函数实现，方便 mock 测试.
func (sc *Slurmctlc) WithExecCommand(exec ExecCommandFunc) *Slurmctlc {
	sc.execCommand = exec
	return sc
}

// GetNodes 获取集群中节点信息, 该函数通过执行 sinfo -h -N -o "%N %t %m %c %X %Y %Z %G" 实现数据获取.
// "节点名称(%N) 节点状态(%t) 内存大小(%m), 总cpus(%c) Socket(%X) Cores(%Y) Threads(%Z)"
// 可选过滤：partition(-p)、status(-t)、nodes(-n)
func (sc *Slurmctlc) GetNodes(ctx context.Context, condPartition, condStatus, condNodes string) (models.Nodes, error) {
	nodes := make(models.Nodes, 0)
	args := []string{"-h", "-N"}
	if condPartition != "" {
		args = append(args, "-p", condPartition)
	}
	if condStatus != "" {
		args = append(args, "-t", condStatus)
	}
	if condNodes != "" {
		args = append(args, "-n", condNodes)
	}
	args = append(args, "-o", "%N %p %t %m %c %X %Y %Z %G")
	cmd := sc.execCommand(ctx, "sinfo", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		sc.logger.Error("failed to exec sinfo command", "output", string(out), "cmd", cmd.String(), "err", err)
		return nil, fmt.Errorf("failed to exec sinfo command")
	}
	// cn1576 short1 drain 128000 36 2 18 1 (null)
	// cn1576 cp1 drain 128000 36 2 18 1 (null)
	// cn1576 all drain 128000 36 2 18 1 (null)
	// cn1576 long1 drain 128000 36 2 18 1 (null)
	// cn1577 short1 alloc 128000 36 2 18 1 (null)
	// cn1577 cp1 alloc 128000 36 2 18 1 (null)
	// cn1577 all alloc 128000 36 2 18 1 (null)
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 7 {
			sc.logger.Warn("invalid sinfo output line, skip", "line", line)
			continue
		}
		memory, _ := strconv.Atoi(fields[3])
		cpus, _ := strconv.Atoi(fields[4])
		socket, _ := strconv.Atoi(fields[5])
		cores, _ := strconv.Atoi(fields[6])
		threads, _ := strconv.Atoi(fields[7])
		if node, ok := nodes[fields[0]]; ok {
			node.Partition = node.Partition + "," + fields[1]
			continue
		}
		nodes[fields[0]] = models.Node{
			Name:      fields[0],
			Partition: fields[1],
			State:     fields[2],
			Memory:    memory,
			CPUs:      cpus,
			Socket:    socket,
			Cores:     cores,
			Threads:   threads,
			GPU:       fields[8],
		}
	}

	return nodes, nil
}

// // GetRunningJobNum 获取当前运行中的作业数, 该函数通过执行 squeue -h -t R | wc -l 实现数据获取.
// func (sc *Slurmctlc) GetRunningJobNum(ctx context.Context) (int, error) {
// 	cmd := sc.execCommand(ctx, "squeue", "-h", "-t", "R", "|", "wc", "-l")
// 	out, err := cmd.CombinedOutput()
// 	if err != nil {
// 		sc.logger.Error("failed to exec squeue command", "output", string(out), "cmd", cmd.String(), "err", err)
// 		return 0, fmt.Errorf("failed to exec squeue command")
// 	}
// 	outStr := strings.TrimSpace(string(out))
// 	num, err := strconv.Atoi(outStr)
// 	if err != nil {
// 		sc.logger.Error("failed to parse running job num", "output", outStr, "err", err)
// 		return 0, fmt.Errorf("failed to parse running job num")
// 	}
// 	return num, nil
// }

// GetJobs 获取调度队列中作业信息
func (sc *Slurmctlc) GetJobs(ctx context.Context) (models.Jobs, error) {
	return nil, nil
}

type Partition struct {
}

// GetPartitions 获取分区详情.
func (sc *Slurmctlc) GetPartitions(ctx context.Context, partitions []string) ([]map[string]string, error) {
    results := make([]map[string]string, 0)
    if len(partitions) != 0 {
        for _, partition := range partitions {
            cmd := sc.execCommand(ctx, "scontrol", "show", "partition", partition)
            out, err := cmd.CombinedOutput()
            if err != nil {
                sc.logger.Error("failed to exec sinfo command", "output", string(out), "cmd", cmd.String(), "err", err)
                return nil, fmt.Errorf("failed to exec %s", cmd.String())
            }
            results = append(results, parsePartition(string(out))...)
        }
        return results, nil
    }

    // 获取所有分区
    cmd := sc.execCommand(ctx, "scontrol", "show", "partition")
    out, err := cmd.CombinedOutput()
    if err != nil {
        sc.logger.Error("failed to exec sinfo command", "output", string(out), "cmd", cmd.String(), "err", err)
        return nil, fmt.Errorf("failed to exec %s", cmd.String())
    }

    return parsePartition(string(out)), nil
}

// func (sc *Slurmctlc) GetPartitions(ctx context.Context) ([]string, error) {
// 	partitions := make([]string, 0)
// 	cmd := sc.execCommand(ctx, "sinfo", "-h", "-o", "%P")
// 	out, err := cmd.CombinedOutput()
// 	if err != nil {
// 		sc.logger.Error("failed to exec sinfo command", "output", string(out), "cmd", cmd.String(), "err", err)
// 		return nil, fmt.Errorf("failed to exec sinfo command")
// 	}
// 	scanner := bufio.NewScanner(bytes.NewReader(out))
// 	for scanner.Scan() {
// 		line := scanner.Text()
// 		fields := strings.Fields(line)
// 		if len(fields) < 1 {
// 			sc.logger.Warn("invalid sinfo output line, skip", "line", line)
// 			continue
// 		}
// 		partitions = append(partitions, fields[0])
// 	}
// 	return partitions, nil
// }

// parseParttion 解析 scontrol show partition 的输出为一个或多个 partition 字段映射。
// 输入可包含多个分区，分区之间通常以空行分隔；每行可能包含多个以空格分隔的 key=value 对。
// 返回按出现顺序的分区切片，每个分区以 map[string]string 表示。
func parsePartition(content string) []map[string]string {
	parts := make([]map[string]string, 0)
	current := make(map[string]string)

	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// 空行表示一个分区的结束
		if trimmed == "" {
			if len(current) > 0 {
				parts = append(parts, current)
				current = make(map[string]string)
			}
			continue
		}

		// 一行可能有多个 key=value，以空白分隔
		tokens := strings.Fields(trimmed)
		for _, tok := range tokens {
			if eq := strings.IndexByte(tok, '='); eq >= 0 {
				key := tok[:eq]
				val := tok[eq+1:]
				// 若遇到新的 PartitionName 且当前分区已存在 PartitionName，则视为新分区开始
				if key == "PartitionName" && len(current) > 0 && current["PartitionName"] != "" {
					parts = append(parts, current)
					current = make(map[string]string)
				}
				current[key] = val
			}
		}
	}

	// 文件结尾若仍有未提交的分区
	if len(current) > 0 {
		parts = append(parts, current)
	}

	return parts
}
