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

// Package-level default Client for convenience wiring.
var defaultClient *Client

// SetDefault sets the package-level default SlurmDB Client.
func SetDefault(c *Client) { defaultClient = c }

// Default returns the package-level default SlurmDB Client.
func Default() *Client { return defaultClient }

// ExecCommandFunc 定义 exec.CommandContext 的函数签名，方便 mock 测试.
type ExecCommandFunc func(ctx context.Context, name string, args ...string) *exec.Cmd

// Client 提供使用命令与 slurmctld 交互的功能.
type Client struct {
	execCommand ExecCommandFunc
	logger      *slog.Logger
}

func (c *Client) Set(exec ExecCommandFunc, logger *slog.Logger) *Client {
	c.execCommand = exec
	c.logger = logger
	return c
}

// GetNodes 获取集群中节点信息, 该函数通过执行 sinfo -h -N -o "%N %P %t %m %c %X %Y %Z %G" 实现数据获取.
// "节点名称(%N) 所属分区(%P) 节点状态(%t) 内存大小(%m), 总cpus(%c) Socket(%X) Cores(%Y) Threads(%Z) Tres(%G)"
// 可选过滤：partition(-p)
func (sc *Client) GetNodes(ctx context.Context, condPartition string) (models.Nodes, error) {
	nodes := make(models.Nodes, 0)
	args := []string{"-h", "-N"}
	if condPartition != "" {
		args = append(args, "-p", condPartition)
	}
	args = append(args, "-o", "%N %P %t %m %c %X %Y %Z %G")
	cmd := sc.execCommand(ctx, "sinfo", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		sc.logger.Error("failed to exec sinfo command", "output", string(out), "cmd", cmd.String(), "err", err)
		return nil, fmt.Errorf("failed to exec sinfo command")
	}
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
		node, ok := nodes[fields[0]]
		if !ok {
			nodes[fields[0]] = &models.Node{
				Name:      fields[0],
				Partition: make([]string, 0),
				State:     fields[2],
				Memory:    memory,
				CPUs:      cpus,
				Socket:    socket,
				Cores:     cores,
				Threads:   threads,
				GPU:       fields[8],
			}
			node, _ = nodes[fields[0]]
		}
		node.Partition = append(node.Partition, fields[1])
	}

	return nodes, nil
}

// GetJobs 获取调度队列中作业信息.
// squeue -o "%i %t %u %a %C %N %P %q %r"
// JOBID ST USER ACCOUNT CPUS NODELIST PARTITION QOS REASON
func (sc *Client) GetJobs(ctx context.Context) (models.Jobs, error) {
	jobs := make(models.Jobs, 0)
	cmd := sc.execCommand(ctx, "squeue", "-h", "-o", "%i|%t|%u|%a|%C|%N|%P|%q|%r")
	out, err := cmd.CombinedOutput()
	if err != nil {
		sc.logger.Error("unable to get all jobs in scheduling queue", "output", string(out), "cmd", cmd.String(), "err", err)
		return nil, fmt.Errorf("failed to exec squeue command")
	}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "|")
		if len(fields) != 9 {
			sc.logger.Warn("invalid squeue output line, skip", "line", line)
			continue
		}
		jobs = append(jobs, models.Job{
			Jobid:     fields[0],
			State:     fields[1],
			User:      fields[2],
			Account:   fields[3],
			CPUs:      fields[4],
			Nodelist:  fields[5],
			Partition: fields[6],
			QoS:       fields[7],
			Reason:    fields[8],
		})
	}

	return jobs, nil
}

func (c *Client) GetJob(ctx context.Context, jobid string) (*models.Job, error) {
	cmd := c.execCommand(ctx, "squeue", "-h", "-j", jobid, "-o", "%i|%t|%u|%a|%C|%N|%P|%q|%r")
	out, err := cmd.CombinedOutput()
	if err != nil {
		c.logger.Error("unable to get job in scheduling queue", "output", string(out), "cmd", cmd.String(), "err", err)
		return nil, fmt.Errorf("unable to get job in scheduling queue")
	}

	fields := strings.Split(strings.TrimSpace(string(out)), "|")
	if len(fields) != 9 {
		c.logger.Warn("invalid squeue output line, skip", "line", string(out))
		return nil, fmt.Errorf("invalid squeue output line, skip")
	}
	job := &models.Job{
		Jobid:     fields[0],
		State:     fields[1],
		User:      fields[2],
		Account:   fields[3],
		CPUs:      fields[4],
		Nodelist:  fields[5],
		Partition: fields[6],
		QoS:       fields[7],
		Reason:    fields[8],
	}

	return job, nil
}

func (c *Client) GetStepsOfJob(ctx context.Context, jobid string) (models.Steps, error) {
	steps := make(models.Steps, 0)
	cmd := c.execCommand(ctx, "squeue", "-s", "-h", "-j", jobid, "-O", "stepid,stepname,stepstate")
	out, err := cmd.CombinedOutput()
	if err != nil {
		c.logger.Error("unable to execute command", "output", string(out), "cmd", cmd.String(), "err", err)
		return nil, fmt.Errorf("failed to exec sinfo command")
	}

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) != 3 {
			c.logger.Warn("invalid squeue output line, skip", "line", line)
			continue
		}
		steps = append(steps, models.Step{
			ID:    fields[0],
			Name:  fields[1],
			State: fields[2],
		})
	}

	return steps, nil
}

// GetPartitions 获取分区详情.
func (c *Client) GetPartitions(ctx context.Context) (models.Partitions, error) {
	// 获取所有分区
	cmd := c.execCommand(ctx, "scontrol", "show", "partition")
	out, err := cmd.CombinedOutput()
	if err != nil {
		c.logger.Error("unable to get all partitions's information", "output", string(out), "cmd", cmd.String(), "err", err)
		return nil, fmt.Errorf("failed to exec %s", cmd.String())
	}

	return parsePartitions(string(out)), nil
}

func (c *Client) GetPartition(ctx context.Context, name string) (models.Partition, error) {
	cmd := c.execCommand(ctx, "scontrol", "show", "partition", name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// TODO 分区不存在的时候也会保存.
		c.logger.Error("unable to get partition information", "output", string(out), "cmd", cmd.String(), "err", err)
		return nil, fmt.Errorf("failed to exec %s", cmd.String())
	}

	return parsePartition(string(out)), nil
}

// parseParttion 解析 scontrol show partition 的输出为一个或多个 partition 字段映射。
// 输入可包含多个分区，分区之间通常以空行分隔；每行可能包含多个以空格分隔的 key=value 对。
// 返回按出现顺序的分区切片，每个分区以 map[string]string 表示。
func parsePartitions(content string) models.Partitions {
	parts := make(models.Partitions, 0)
	current := make(models.Partition)

	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// 空行表示一个分区的结束
		if trimmed == "" {
			if len(current) > 0 {
				parts = append(parts, current)
				current = make(models.Partition)
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

func parsePartition(content string) models.Partition {
	current := make(models.Partition)

	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// 一行可能有多个 key=value，以空白分隔
		tokens := strings.Fields(trimmed)
		for _, tok := range tokens {
			if eq := strings.IndexByte(tok, '='); eq >= 0 {
				key := tok[:eq]
				val := tok[eq+1:]
				current[key] = val
			}
		}
	}

	return current
}
