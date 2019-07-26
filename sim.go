package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Object struct {
	Key  string
	Sz   uint64
	Freq uint64
}

type Lambda struct {
	Kvs     map[string]*Object
	MemUsed uint64
}

type Proxy struct {
	Id         string
	LambdaPool []Lambda
	Map        map[string][]int
}

type Record struct {
	Key            string
	Sz             uint64
	Timestamp      string
	TimestampHr    uint64
	Timestamp15min uint64
}

type Member string

func (m Member) String() string {
	//return strconv.Atoi(m)
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// random will generate random sequence within the lambda stores
// index and get top n id
func random(numLambdas int, numChunks int) []int {
	rand.Seed(time.Now().UnixNano())
	return rand.Perm(numLambdas)[:numChunks]
}

func perform(p Proxy, rec Record, reuseTimeline [][]int, memArr []int, opts *Options) {
	log.Println("Key:", rec.Key, "mapped to Proxy:", p.Id)
	if val, ok := p.Map[rec.Key]; ok {
		// if key exists
		for _, idx := range val {
			obj := p.LambdaPool[idx].Kvs[rec.Key]
			obj.Sz = rec.Sz / uint64(opts.NumDataShards)
			obj.Freq++

			// and then, update the Lambda timeline table
			proxyIdx, _ := strconv.Atoi(p.Id)
			// generate the lambda index to locate the exact lambda touched
			lambdaIdx := proxyIdx*opts.NumLambdasPerProxy + idx
			timestampHr := rec.TimestampHr
			// simply self-increment
			reuseTimeline[lambdaIdx][timestampHr]++
		}
		log.Println("[Hit] Key:", rec.Key, "existing... On Lambda:", val)
	} else {
		// if key does not exist, generate the index array holding
		// indexes of the destination lambdas
		index := random(opts.NumLambdasPerProxy, opts.NumDataShards+opts.NumParityShards)
		p.Map[rec.Key] = index
		for _, idx := range index {
			p.LambdaPool[idx].Kvs[rec.Key] = &Object{
				Key:  rec.Key,
				Sz:   rec.Sz / uint64(opts.NumDataShards),
				Freq: 0,
			}
			p.LambdaPool[idx].MemUsed += rec.Sz / uint64(opts.NumDataShards)

			// and then, update the Lambda mem table
			proxyIdx, _ := strconv.Atoi(p.Id)
			// generate the lambda index to locate the exact lambda touched
			lambdaIdx := proxyIdx*opts.NumLambdasPerProxy + idx
			timestampHr := rec.TimestampHr
			//memTimeline[lambdaIdx][timestampHr] += int(rec.Sz) / 1048576
			if timestampHr <= 99 {
				memArr[lambdaIdx] += int(rec.Sz) / 1048576
			}
			// though this is a miss (first-time SET)
			reuseTimeline[lambdaIdx][timestampHr]++
		}
		log.Println("[Miss] Key:", rec.Key, "not existing... Access Lambda:", index)
	}
}

func printAccessHistory(tl [][]int, outputPath string, numHours int) {
	f, err := os.Create(outputPath)
	if err != nil {
		log.Fatal("Failed to open file")
		os.Exit(1)
	}
	w := bufio.NewWriter(f)
	for i, _ := range tl {
		//fmt.Fprintf(w, "Lambda[%d]: ", i)
		//for j, _ := range tl[i] {
		for j := 0; j < numHours-1; j++ {
			fmt.Fprintf(w, "%d,  ", tl[i][j])
		}
		fmt.Fprintf(w, "%d\n", tl[i][numHours-1])
	}
	w.Flush()
	f.Close()
}

func printMemUsage(arr []int, outputPath string) {
	f, err := os.Create(outputPath)
	if err != nil {
		log.Fatal("Failed to open file")
		os.Exit(1)
	}
	w := bufio.NewWriter(f)
	for i, _ := range arr {
		fmt.Fprintf(w, "%d\n", arr[i])
		//fmt.Fprintf(w, "%d\n", tl[i][numHours-1])
	}
	//fmt.Fprintf(w, "\n")
	w.Flush()
	f.Close()
}

func initProxies(nProxies int, nLambdasPerProxy int) ([]Proxy, *consistent.Consistent) {
	proxies := make([]Proxy, nProxies)
	members := []consistent.Member{}
	for i, _ := range proxies {
		proxies[i].Id = strconv.Itoa(i)
		proxies[i].LambdaPool = make([]Lambda, nLambdasPerProxy)
		proxies[i].Map = make(map[string][]int)
		for j, _ := range proxies[i].LambdaPool {
			proxies[i].LambdaPool[j].Kvs = make(map[string]*Object)
			proxies[i].LambdaPool[j].MemUsed = 0
		}
		member := Member(proxies[i].Id)
		log.Println("id:", proxies[i].Id, "id:", i)
		members = append(members, member)
	}
	log.Println("members size:", len(members))

	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	ring := consistent.New(members, cfg)

	return proxies, ring
}

func initTimeline(numLambdas int, numHours int) [][]int {
	tlArr := make([][]int, numLambdas)
	for i, _ := range tlArr {
		tlArr[i] = make([]int, numHours)
	}

	return tlArr
}

type Options struct {
	InputCsv           string
	ReuseOutputCsv     string
	MemOutputCsv       string
	NumProxies         int
	NumLambdasPerProxy int
	NumHours           int
	NumDataShards      int
	NumParityShards    int
}

var DefaultOptions = &Options{
	InputCsv:           "input.csv",
	ReuseOutputCsv:     "output.csv",
	MemOutputCsv:       "output2.csv",
	NumProxies:         2,
	NumLambdasPerProxy: 32,
	NumHours:           1800,
	NumDataShards:      10,
	NumParityShards:    2,
}

func helpInfo() {
	fmt.Println("Usage: ./sim [options]")
	fmt.Println("Option list: ")
	fmt.Println("  -input [Input file path]: path to the input csv file")
	fmt.Println("  -reuseOutput [Output file path]: path to the reuse output csv file")
	fmt.Println("  -memOutput [Output file path]: path to the mem output csv file")
	fmt.Println("  -s [NUMBER]: number of proxy servers")
	fmt.Println("  -l [NUMBER]: number of lambdas per proxy")
	fmt.Println("  -hr [NUMBER]: duration of the traces (hours)")
	fmt.Println("  -d [NUMBER]: number of data shards for RS erasure coding")
	fmt.Println("  -p [NUMBER]: number of parity shards for RS erasure coding")
	fmt.Println("  -h: print out help info?")
}

func main() {
	var printInfo bool
	flag.BoolVar(&printInfo, "h", false, "help info?")

	option := DefaultOptions
	flag.StringVar(&option.InputCsv, "input", "input.csv", "input file path")
	flag.StringVar(&option.ReuseOutputCsv, "reuseOutput", "output.csv", "output file path")
	flag.StringVar(&option.MemOutputCsv, "memOutput", "output.csv", "output file path")
	flag.IntVar(&option.NumProxies, "s", 2, "number of proxies")
	flag.IntVar(&option.NumLambdasPerProxy, "l", 32, "number of lambdas per proxy")
	flag.IntVar(&option.NumHours, "hr", 1800, "duration of the traces (hours)")
	flag.IntVar(&option.NumDataShards, "d", 10, "number of data shards for EC")
	flag.IntVar(&option.NumParityShards, "p", 2, "number of parity shards for EC")
	flag.Parse()

	if printInfo {
		helpInfo()
		os.Exit(0)
	}

	traceFile, err := os.Open(option.InputCsv)
	if err != nil {
		log.Fatal("Failed to open file")
		os.Exit(1)
	}
	defer traceFile.Close()

	reader := csv.NewReader(bufio.NewReader(traceFile))
	proxies, ring := initProxies(option.NumProxies, option.NumLambdasPerProxy)
	reuseTimeline := initTimeline(option.NumProxies*option.NumLambdasPerProxy, option.NumHours)
	//memTimeline := initTimeline(option.NumProxies*option.NumLambdasPerProxy, option.NumHours)
	memArr := make([]int, option.NumProxies*option.NumLambdasPerProxy)

	var rec Record
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		//sz, _ := strconv.ParseUint(line[9], 10, 64)
		sz, _ := strconv.ParseFloat(line[9], 64)
		tsHr, _ := strconv.ParseFloat(line[12], 64)
		ts15min, _ := strconv.ParseFloat(line[14], 64)
		rec = Record{
			Key:            line[6],
			Sz:             uint64(sz),
			Timestamp:      line[11],
			TimestampHr:    uint64(tsHr),
			Timestamp15min: uint64(ts15min),
		}
		//log.Println(line)
		//log.Println("parsed record:", rec)

		member := ring.LocateKey([]byte(rec.Key))
		hostId := member.String()
		id, _ := strconv.Atoi(hostId)
		//log.Println("Key:", rec.Key, "mapped to Proxy id:", id)
		perform(proxies[id], rec, reuseTimeline, memArr, option)
	}

	printAccessHistory(reuseTimeline, option.ReuseOutputCsv, option.NumHours)
	printMemUsage(memArr, option.MemOutputCsv)
}
