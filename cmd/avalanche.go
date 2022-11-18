// Copyright 2022 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/common/version"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/prometheus-community/avalanche/metrics"
	"github.com/prometheus-community/avalanche/pkg/download"
)

var (
	valueInterval       = kingpin.Flag("value-interval", "Change series values every {interval} seconds.").Default("30").Int()
	port                = kingpin.Flag("port", "Port to serve at").Default("9001").Int()
	remoteURL           = kingpin.Flag("remote-url", "URL to send samples via remote_write API.").URL()
	remotePprofURLs     = kingpin.Flag("remote-pprof-urls", "a list of urls to download pprofs during the remote write: --remote-pprof-urls=http://127.0.0.1:10902/debug/pprof/heap --remote-pprof-urls=http://127.0.0.1:10902/debug/pprof/profile").URLList()
	remotePprofInterval = kingpin.Flag("remote-pprof-interval", "how often to download pprof profiles.When not provided it will download a profile once before the end of the test.").Duration()
	remoteBatchSize     = kingpin.Flag("remote-batch-size", "how many samples to send with each remote_write API request.").Default("2000").Int()
	remoteRequestCount  = kingpin.Flag("remote-requests-count", "how many requests to send in total to the remote_write API.").Default("100").Int()
	remoteReqsInterval  = kingpin.Flag("remote-write-interval", "delay between each remote write request.").Default("100ms").Duration()
	remoteTenant        = kingpin.Flag("remote-tenant", "Tenant ID to include in remote_write send").Default("0").String()
	tlsClientInsecure   = kingpin.Flag("tls-client-insecure", "Skip certificate check on tls connection").Default("false").Bool()
	remoteTenantHeader  = kingpin.Flag("remote-tenant-header", "Tenant ID to include in remote_write send. The default, is the default tenant header expected by Cortex.").Default("X-Scope-OrgID").String()
	topicCount          = kingpin.Flag("topic-count", "Topic Count").Default("10").Int()
	partitionPerTopic   = kingpin.Flag("partition-per-topic", "Partition Per Topic").Default("3").Int()
	producerPerTopic    = kingpin.Flag("producer-per-topic", "Producer Per Topic").Default("1").Int()
	groupCount          = kingpin.Flag("group-count", "Group Count").Default("5").Int()
	instanceLabel       = kingpin.Flag("instance-label", "Instance Label").String()
	topicStartingNumber = kingpin.Flag("topic-staring-number", "Topic Name starting index").Default("0").Int()
	groupStartingNumber = kingpin.Flag("group-staring-number", "Group Name starting index").Default("0").Int()
	replicationFactor   = kingpin.Flag("replication-factor", "Replication factor of topics").Default("2").Int()
)

func main() {
	kingpin.Version(version.Print("avalanche"))
	log.SetFlags(log.Ltime | log.Lshortfile) // Show file name and line in logs.
	kingpin.CommandLine.Help = "avalanche - metrics test server"
	kingpin.Parse()

	stop := make(chan struct{})
	defer close(stop)
	updateNotify, err := metrics.RunMetrics(*replicationFactor, *topicStartingNumber, *groupStartingNumber, *topicCount, *partitionPerTopic, *producerPerTopic, *groupCount, *valueInterval, *instanceLabel, stop)
	if err != nil {
		log.Fatal(err)
	}

	if *remoteURL != nil {
		if (**remoteURL).Host == "" || (**remoteURL).Scheme == "" {
			log.Fatal("remote host and scheme can't be empty")
		}
		if *remoteBatchSize <= 0 {
			log.Fatal("remote send batch size should be more than zero")
		}

		config := &metrics.ConfigWrite{
			URL:             **remoteURL,
			RequestInterval: *remoteReqsInterval,
			BatchSize:       *remoteBatchSize,
			RequestCount:    *remoteRequestCount,
			UpdateNotify:    updateNotify,
			Tenant:          *remoteTenant,
			TLSClientConfig: tls.Config{
				InsecureSkipVerify: *tlsClientInsecure,
			},
			TenantHeader: *remoteTenantHeader,
		}

		// Collect Pprof during the write only if not collecting within a regular interval.
		if *remotePprofInterval == 0 {
			config.PprofURLs = *remotePprofURLs
		}

		var (
			wg   sync.WaitGroup
			done = make(chan struct{})
		)
		if *remotePprofInterval > 0 {
			if len(*remotePprofURLs) == 0 {
				log.Fatal("remote profiling interval specified wihout any remote pprof urls")
			}
			rand.Seed(time.Now().UnixNano())
			suffix := rand.Intn(1000)
			go func() {
				ticker := time.NewTicker(*remotePprofInterval)
				var dur time.Duration
			loop:
				for {
					<-ticker.C
					select {
					case <-done: // Prevents a panic when calling wg.Add(1) after calling wg.Wait().
						break loop
					default:
					}
					dur += *remotePprofInterval
					wg.Add(1)
					download.URLs(*remotePprofURLs, strconv.Itoa(suffix)+"-"+dur.String())
					wg.Done()
				}
			}()

		}
		// First cut: just send the metrics once then exit
		err := metrics.SendRemoteWrite(config)
		if err != nil {
			log.Fatal(err)
		}
		if *remotePprofInterval > 0 {
			done <- struct{}{}
			wg.Wait()
		}
		return
	}

	fmt.Printf("Serving ur metrics at localhost:%v/metrics\n", *port)
	err = metrics.ServeMetrics(*port)
	if err != nil {
		log.Fatal(err)
	}
}
