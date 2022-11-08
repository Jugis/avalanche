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

package metrics

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	health "github.com/nelkinda/health-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	promRegistry     = prometheus.NewRegistry() // local Registry so we don't get Go metrics, etc.
	valGenerator     = rand.New(rand.NewSource(time.Now().UnixNano()))
	metrics          = make([]*prometheus.GaugeVec, 0)
	topicMetrics     = make([]*prometheus.GaugeVec, 0)
	partitionMetrics = make([]*prometheus.GaugeVec, 0)
	groupMetrics     = make([]*prometheus.GaugeVec, 0)
	producerMetrics  = make([]*prometheus.GaugeVec, 0)
	metricsMux       = &sync.Mutex{}
)

func registerKafkaMetrics() {
	metrics = make([]*prometheus.GaugeVec, 18)
	// Metrics with topic cardinality
	topicMetrics = make([]*prometheus.GaugeVec, 6)
	topicIdx := 0
	topicMetrics[topicIdx] = registerKafkaGaugeMetrics("topic_bytesinpersec_total", "Topic BytesInPerSec Count", []string{"instance", "topic"})
	topicIdx++
	topicMetrics[topicIdx] = registerKafkaGaugeMetrics("topic_bytesoutpersec_total", "Topic BytesOutPerSec Count", []string{"instance", "topic"})
	topicIdx++
	topicMetrics[topicIdx] = registerKafkaGaugeMetrics("topic_messagesinpersec_total", "Topic MessagesInPerSec Count", []string{"instance", "topic"})
	topicIdx++
	topicMetrics[topicIdx] = registerKafkaGaugeMetrics("topic_bytesinpersec_fifteenminuterate", "Topic BytesInPerSec 15min Rate", []string{"instance", "topic"})
	topicIdx++
	topicMetrics[topicIdx] = registerKafkaGaugeMetrics("topic_bytesoutpersec_fifteenminuterate", "Topic BytesOutPerSec 15min Rate", []string{"instance", "topic"})
	topicIdx++
	topicMetrics[topicIdx] = registerKafkaGaugeMetrics("topic_messagesinpersec_fifteenminuterate", "Topic MessagesInPerSec 15min Rate", []string{"instance", "topic"})

	// Metrics with topic x partition cardinality
	partitionMetrics = make([]*prometheus.GaugeVec, 8)
	partitionIdx := 0
	partitionMetrics[partitionIdx] = registerKafkaGaugeMetrics("topic_partition_bytesinpersec_total", "Topic Partition BytesInPerSec Count", []string{"instance", "topic", "partition"})
	partitionIdx++
	partitionMetrics[partitionIdx] = registerKafkaGaugeMetrics("topic_partition_bytesoutpersec_total", "Topic Partition BytesOutPerSec Count", []string{"instance", "topic", "partition"})
	partitionIdx++
	partitionMetrics[partitionIdx] = registerKafkaGaugeMetrics("topic_partition_bytesinpersec_fifteenminuterate", "Topic Partition BytesInPerSec 15min Rate", []string{"instance", "topic", "partition"})
	partitionIdx++
	partitionMetrics[partitionIdx] = registerKafkaGaugeMetrics("topic_partition_bytesoutpersec_fifteenminuterate", "Topic Partition BytesOutPerSec 15min Rate", []string{"instance", "topic", "partition"})
	partitionIdx++
	partitionMetrics[partitionIdx] = registerKafkaGaugeMetrics("topic_partition_messagesinpersec_fifteenminuterate", "Topic Partition MessagesInPerSec 15min Rate", []string{"instance", "topic", "partition"})
	partitionIdx++
	partitionMetrics[partitionIdx] = registerKafkaGaugeMetrics("topic_partition_replicascount", "Topic Partition ReplicasCount", []string{"instance", "topic", "partition"})
	partitionIdx++
	partitionMetrics[partitionIdx] = registerKafkaGaugeMetrics("topic_partition_insyncreplicascount", "Topic Partition InSyncReplicasCount", []string{"instance", "topic", "partition"})
	partitionIdx++
	partitionMetrics[partitionIdx] = registerKafkaGaugeMetrics("topic_partition_underreplicated", "Topic Partition UnderReplicated", []string{"instance", "topic", "partition"})

	// Metrics with group cardinality
	groupMetrics = make([]*prometheus.GaugeVec, 2)
	groupIdx := 0
	groupMetrics[groupIdx] = registerKafkaGaugeMetrics("group_committed_offset", "Group Committed Offset", []string{"instance", "group"})
	groupIdx++
	groupMetrics[groupIdx] = registerKafkaGaugeMetrics("group_lag", "Group Lag", []string{"instance", "group"})

	// Metrics with topic x producer cardinality
	producerMetrics = make([]*prometheus.GaugeVec, 1)
	producerIdx := 0
	producerMetrics[producerIdx] = registerKafkaGaugeMetrics("broker_producer_messagesinpersec_total", "Producer MessagesInPerSec Count", []string{"instance", "topic", "clientId"})

	metrics = append(append(append(topicMetrics, partitionMetrics...), groupMetrics...), producerMetrics...)
}

func registerKafkaGaugeMetrics(name, help string, labelNames []string) *prometheus.GaugeVec {
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: name,
		Help: help,
	}, labelNames)
	promRegistry.MustRegister(gauge)
	return gauge
}

func registerMetrics(metricCount, metricLength, metricCycle int, labelKeys []string) {
	metrics = make([]*prometheus.GaugeVec, metricCount)
	for idx := 0; idx < metricCount; idx++ {
		gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: fmt.Sprintf("avalanche_metric_%s_%v_%v", strings.Repeat("m", metricLength), metricCycle, idx),
			Help: "A tasty metric morsel",
		}, append([]string{"series_id", "cycle_id"}, labelKeys...))
		promRegistry.MustRegister(gauge)
		metrics[idx] = gauge
	}
}

func unregisterMetrics() {
	for _, metric := range metrics {
		promRegistry.Unregister(metric)
	}
}

func seriesLabels(seriesID, cycleID int, labelKeys, labelValues []string) prometheus.Labels {
	labels := prometheus.Labels{
		"series_id": fmt.Sprintf("%v", seriesID),
		"cycle_id":  fmt.Sprintf("%v", cycleID),
	}

	for idx, key := range labelKeys {
		labels[key] = labelValues[idx]
	}

	return labels
}

func deleteValues(labelKeys, labelValues []string, seriesCount, seriesCycle int) {
	for _, metric := range metrics {
		for idx := 0; idx < seriesCount; idx++ {
			labels := seriesLabels(idx, seriesCycle, labelKeys, labelValues)
			metric.Delete(labels)
		}
	}
}

func cycleKafkaValues(topicCount, partitionPerTopic, producerPerTopic, groupCount int, instanceLabel string) {
	for _, metric := range topicMetrics {
		for idx := 0; idx < topicCount; idx++ {
			labels := prometheus.Labels{"instance": instanceLabel, "topic": fmt.Sprintf("topic_%v", idx)}
			metric.With(labels).Set(float64(valGenerator.Intn(100)))
		}
	}

	for _, metric := range partitionMetrics {
		for topicIdx := 0; topicIdx < topicCount; topicIdx++ {
			for partitionIdx := 0; partitionIdx < partitionPerTopic; partitionIdx++ {
				labels := prometheus.Labels{"instance": instanceLabel, "topic": fmt.Sprintf("topic_%v", topicIdx), "partition": fmt.Sprintf("%v", partitionIdx)}
				metric.With(labels).Set(float64(valGenerator.Intn(100)))
			}
		}
	}

	for _, metric := range groupMetrics {
		for idx := 0; idx < groupCount; idx++ {
			labels := prometheus.Labels{"instance": instanceLabel, "group": fmt.Sprintf("group_%v", idx)}
			metric.With(labels).Set(float64(valGenerator.Intn(100)))
		}
	}

	producerIdx := 0
	for _, metric := range producerMetrics {
		for topicIdx := 0; topicIdx < topicCount; topicIdx++ {
			for producers := 0; producers < producerPerTopic; producers++ {
				labels := prometheus.Labels{"instance": instanceLabel, "topic": fmt.Sprintf("topic_%v", topicIdx), "clientId": fmt.Sprintf("producer_%v", producerIdx)}
				metric.With(labels).Set(float64(valGenerator.Intn(100)))
			}
		}
	}
}

// RunMetrics creates a set of Prometheus test series that update over time
func RunMetrics(topicCount, partitionPerTopic, producerPerTopic, groupCount, valueInterval int, instanceLabel string, stop chan struct{}) (chan struct{}, error) {
	registerKafkaMetrics()
	cycleKafkaValues(topicCount, partitionPerTopic, producerPerTopic, groupCount, instanceLabel)
	valueTick := time.NewTicker(time.Duration(valueInterval) * time.Second)
	updateNotify := make(chan struct{}, 1)

	go func() {
		for tick := range valueTick.C {
			fmt.Printf("%v: refreshing metric values\n", tick)
			metricsMux.Lock()
			cycleKafkaValues(topicCount, partitionPerTopic, producerPerTopic, groupCount, instanceLabel)
			metricsMux.Unlock()
			select {
			case updateNotify <- struct{}{}:
			default:
			}
		}
	}()

	go func() {
		<-stop
		valueTick.Stop()
	}()

	return updateNotify, nil
}

// ServeMetrics serves a prometheus metrics endpoint with test series
func ServeMetrics(port int) error {
	http.Handle("/metrics", promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{}))
	h := health.New(health.Health{})
	http.HandleFunc("/health", h.Handler)
	err := http.ListenAndServe(fmt.Sprintf(":%v", port), nil)
	if err != nil {
		return err
	}

	return nil
}
