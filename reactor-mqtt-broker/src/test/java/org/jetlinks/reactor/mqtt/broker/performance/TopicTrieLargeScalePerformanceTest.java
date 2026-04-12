/*
 * Copyright 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.mqtt.broker.performance;

import org.jetlinks.reactor.mqtt.Topic;
import org.jetlinks.reactor.mqtt.TopicTrie;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * TopicTrie 大规模主题性能测试。
 *
 * <p>该测试位于 performance 目录，默认不会被 surefire 执行。
 * 用于手动评估大量唯一 topic 场景下的装载与匹配耗时。</p>
 */
class TopicTrieLargeScalePerformanceTest {

    private static final int LOOKUP_ITERATIONS = 5_000;
    private static final int WARMUP_ITERATIONS = 2_000;

    @Test
    void shouldMeasureLargeScaleTopicLookup() {
        runExactAndLightWildcardScenario(10_000);
        runExactAndLightWildcardScenario(100_000);
        runExactAndLightWildcardScenario(300_000);
    }

    @Test
    void shouldMeasureWildcardDenseTopicLookup() {
        runWildcardDenseScenario(10_000);
        runWildcardDenseScenario(100_000);
        runWildcardDenseScenario(300_000);
    }

    private void runExactAndLightWildcardScenario(int topicCount) {
        TopicTrie<String> trie = new TopicTrie<>();

        long insertStart = System.nanoTime();
        for (int i = 0; i < topicCount; i++) {
            trie.addSubscription(uniqueTopic(i), "client" + i);
        }
        trie.addSubscription(Topic.of("sensor/+/device100/#").getLevels(), "wildcard1");
        trie.addSubscription(Topic.of("sensor/room100/#").getLevels(), "wildcard2");
        long insertNanos = System.nanoTime() - insertStart;

        String[] query = Topic.of("sensor/room100/device100/metric100").getLevels();
        LookupMetrics metrics = measureLookup(trie, query, topicCount, "exact+light-wildcard", insertNanos);

        assertEquals(3, metrics.matches(), "Expected exact topic plus 2 wildcard matches");
        assertEquals((long) LOOKUP_ITERATIONS * 3, metrics.checksum(), "Each lookup should find 3 matches");
    }

    private void runWildcardDenseScenario(int topicCount) {
        TopicTrie<String> trie = new TopicTrie<>();

        long insertStart = System.nanoTime();
        for (int i = 0; i < topicCount; i++) {
            trie.addSubscription(uniqueTopic(i), "client" + i);
        }
        for (int room = 0; room < 256; room++) {
            trie.addSubscription(Topic.of("sensor/room" + room + "/#").getLevels(), "roomHash" + room);
        }
        for (int device = 0; device < 256; device++) {
            trie.addSubscription(Topic.of("sensor/+/device" + device + "/#").getLevels(), "deviceHash" + device);
        }
        for (int metric = 0; metric < 256; metric++) {
            trie.addSubscription(Topic.of("sensor/+/+/metric" + metric).getLevels(), "metricPlus" + metric);
        }
        trie.addSubscription(Topic.of("sensor/#").getLevels(), "globalHash");
        long insertNanos = System.nanoTime() - insertStart;

        String[] query = Topic.of("sensor/room100/device100/metric100").getLevels();
        LookupMetrics metrics = measureLookup(trie, query, topicCount, "wildcard-dense", insertNanos);

        assertEquals(5, metrics.matches(), "Expected exact topic plus 4 wildcard matches");
        assertEquals((long) LOOKUP_ITERATIONS * 5, metrics.checksum(), "Each lookup should find 5 matches");
    }

    private LookupMetrics measureLookup(TopicTrie<String> trie,
                                        String[] query,
                                        int topicCount,
                                        String scenarioName,
                                        long insertNanos) {
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            trie.findMatches(query);
        }

        long lookupStart = System.nanoTime();
        Set<String> matches = Set.of();
        long checksum = 0;
        for (int i = 0; i < LOOKUP_ITERATIONS; i++) {
            matches = trie.findMatches(query);
            checksum += matches.size();
        }
        long lookupNanos = System.nanoTime() - lookupStart;

        return new LookupMetrics(topicCount, scenarioName, insertNanos, lookupNanos, matches.size(), checksum);
    }

    private record LookupMetrics(int topicCount,
                                 String scenarioName,
                                 long insertNanos,
                                 long lookupNanos,
                                 int matches,
                                 long checksum) {

        private LookupMetrics {
            double insertMs = insertNanos / 1_000_000.0;
            double insertPerOpUs = insertNanos / 1_000.0 / topicCount;
            double lookupAvgUs = lookupNanos / 1_000.0 / LOOKUP_ITERATIONS;
            double lookupQps = 1_000_000.0 / lookupAvgUs;
            System.out.printf(
                    "TopicTrie Large Scale [%s]: count=%d, insert=%.3f ms, insert/op=%.3f us, lookupAvg=%.3f us, lookupQps=%.0f/s, matches=%d, checksum=%d%n",
                    scenarioName,
                    topicCount,
                    insertMs,
                    insertPerOpUs,
                    lookupAvgUs,
                    lookupQps,
                    matches,
                    checksum
            );
        }
    }

    private String[] uniqueTopic(int i) {
        return Topic.of("sensor/room" + i + "/device" + i + "/metric" + i).getLevels();
    }
}
