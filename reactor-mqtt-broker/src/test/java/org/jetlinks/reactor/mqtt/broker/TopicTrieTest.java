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
package org.jetlinks.reactor.mqtt.broker;

import org.jetlinks.reactor.mqtt.ParsedTopic;
import org.jetlinks.reactor.mqtt.TopicTrie;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TopicTrie 单元测试
 *
 * @author PengyuDeng
 */
class TopicTrieTest {

    private TopicTrie<String> trie;

    @BeforeEach
    void setUp() {
        trie = new TopicTrie();
    }

    @Test
    void testExactMatch() {
        trie.addSubscription(ParsedTopic.parse("sensor/temperature").getLevels(), "client1");

        Set<String> matches = trie.findMatches(ParsedTopic.parse("sensor/temperature").getLevels());
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client1"));

        matches = trie.findMatches(ParsedTopic.parse("sensor/humidity").getLevels());
        assertTrue(matches.isEmpty());
    }

    @Test
    void testSingleWildcard() {
        trie.addSubscription(ParsedTopic.parse("sensor/+/temperature").getLevels(), "client1");

        Set<String> matches = trie.findMatches(ParsedTopic.parse("sensor/room1/temperature").getLevels());
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client1"));

        matches = trie.findMatches(ParsedTopic.parse("sensor/room2/temperature").getLevels());
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client1"));

        matches = trie.findMatches(ParsedTopic.parse("sensor/room1/room2/temperature").getLevels());
        assertTrue(matches.isEmpty());

        matches = trie.findMatches(ParsedTopic.parse("sensor/temperature").getLevels());
        assertTrue(matches.isEmpty());
    }

    @Test
    void testMultiWildcard() {
        trie.addSubscription(ParsedTopic.parse("sensor/#").getLevels(), "client1");

        Set<String> matches = trie.findMatches(ParsedTopic.parse("sensor/temperature").getLevels());
        assertEquals(1, matches.size());

        matches = trie.findMatches(ParsedTopic.parse("sensor/room1/temperature").getLevels());
        assertEquals(1, matches.size());

        matches = trie.findMatches(ParsedTopic.parse("sensor/room1/room2/temperature").getLevels());
        assertEquals(1, matches.size());

        matches = trie.findMatches(ParsedTopic.parse("device/temperature").getLevels());
        assertTrue(matches.isEmpty());
    }

    @Test
    void testMultipleSubscribers() {
        trie.addSubscription(ParsedTopic.parse("sensor/temperature").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("sensor/temperature").getLevels(), "client2");
        trie.addSubscription(ParsedTopic.parse("sensor/temperature").getLevels(), "client3");

        Set<String> matches = trie.findMatches(ParsedTopic.parse("sensor/temperature").getLevels());
        assertEquals(3, matches.size());
        assertTrue(matches.contains("client1"));
        assertTrue(matches.contains("client2"));
        assertTrue(matches.contains("client3"));
    }

    @Test
    void testOverlappingSubscriptions() {
        trie.addSubscription(ParsedTopic.parse("sensor/+/temperature").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("sensor/#").getLevels(), "client2");
        trie.addSubscription(ParsedTopic.parse("sensor/room1/temperature").getLevels(), "client3");

        Set<String> matches = trie.findMatches(ParsedTopic.parse("sensor/room1/temperature").getLevels());
        assertEquals(3, matches.size());
        assertTrue(matches.contains("client1"));
        assertTrue(matches.contains("client2"));
        assertTrue(matches.contains("client3"));
    }

    @Test
    void testRemoveSubscription() {
        trie.addSubscription(ParsedTopic.parse("sensor/temperature").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("sensor/temperature").getLevels(), "client2");

        boolean removed = trie.removeSubscription(ParsedTopic.parse("sensor/temperature").getLevels(), "client1");
        assertTrue(removed);

        Set<String> matches = trie.findMatches(ParsedTopic.parse("sensor/temperature").getLevels());
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client2"));

        removed = trie.removeSubscription(ParsedTopic.parse("sensor/temperature").getLevels(), "client2");
        assertTrue(removed);

        matches = trie.findMatches(ParsedTopic.parse("sensor/temperature").getLevels());
        assertTrue(matches.isEmpty());
    }

    @Test
    void testRemoveAllSubscriptions() {
        trie.addSubscription(ParsedTopic.parse("sensor/temp").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("sensor/humidity").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("device/status").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("sensor/temp").getLevels(), "client2");

        trie.removeAll("client1");

        Set<String> matches = trie.findMatches(ParsedTopic.parse("sensor/temp").getLevels());
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client2"));

        matches = trie.findMatches(ParsedTopic.parse("sensor/humidity").getLevels());
        assertTrue(matches.isEmpty());

        matches = trie.findMatches(ParsedTopic.parse("device/status").getLevels());
        assertTrue(matches.isEmpty());
    }

    @Test
    void testGetSubscriberCount() {
        trie.addSubscription(ParsedTopic.parse("sensor/temperature").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("sensor/temperature").getLevels(), "client2");

        assertEquals(2, trie.getSubscriberCount(ParsedTopic.parse("sensor/temperature").getLevels()));
        assertEquals(0, trie.getSubscriberCount(ParsedTopic.parse("sensor/humidity").getLevels()));
    }

    @Test
    void testGetTotalSubscriptionCount() {
        trie.addSubscription(ParsedTopic.parse("sensor/temp").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("sensor/temp").getLevels(), "client2");
        trie.addSubscription(ParsedTopic.parse("sensor/humidity").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("device/#").getLevels(), "client3");

        assertEquals(4, trie.getTotalSubscriptionCount());
    }

    @Test
    void testClear() {
        trie.addSubscription(ParsedTopic.parse("sensor/temp").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("sensor/humidity").getLevels(), "client2");

        trie.clear();

        assertEquals(0, trie.getTotalSubscriptionCount());
        assertTrue(trie.findMatches(ParsedTopic.parse("sensor/temp").getLevels()).isEmpty());
    }

    @Test
    void testEdgeCases() {
        assertThrows(IllegalArgumentException.class, () ->
                trie.addSubscription(ParsedTopic.parse("").getLevels(), "client1"));

        assertThrows(IllegalArgumentException.class, () ->
                trie.addSubscription((String[]) null, "client1"));

        assertThrows(IllegalArgumentException.class, () ->
                trie.addSubscription(ParsedTopic.parse("sensor/temp").getLevels(), null));

        assertThrows(IllegalArgumentException.class, () ->
                trie.addSubscription(ParsedTopic.parse("sensor/#/temp").getLevels(), "client1"));
    }

    @Test
    void testComplexTopics() {
        trie.addSubscription(ParsedTopic.parse("a/b/c/d/e").getLevels(), "client1");
        trie.addSubscription(ParsedTopic.parse("a/+/c/+/e").getLevels(), "client2");
        trie.addSubscription(ParsedTopic.parse("a/#").getLevels(), "client3");

        Set<String> matches = trie.findMatches(ParsedTopic.parse("a/b/c/d/e").getLevels());
        assertEquals(3, matches.size());

        matches = trie.findMatches(ParsedTopic.parse("a/x/c/y/e").getLevels());
        assertEquals(2, matches.size());
        assertTrue(matches.contains("client2"));
        assertTrue(matches.contains("client3"));
    }

    @Test
    void testPerformance() {
        int subscriptionCount = 10_000;
        for (int i = 0; i < subscriptionCount; i++) {
            String topic = "sensor/room" + (i % 100) + "/device" + (i % 50) + "/metric" + (i % 20);
            trie.addSubscription(ParsedTopic.parse(topic).getLevels(), "client" + i);
        }

        long startTime = System.nanoTime();
        Set<String> matches = trie.findMatches(ParsedTopic.parse("sensor/room5/device10/metric3").getLevels());
        long endTime = System.nanoTime();

        double durationMs = (endTime - startTime) / 1_000_000.0;

        System.out.printf("TopicTrie Performance: Found %d matches in %.3f ms (10,000 subscriptions)%n",
                matches.size(), durationMs);

        assertTrue(durationMs < 1.0, "Lookup should be < 1ms, but was: " + durationMs + " ms");
    }

    @Test
    void testPerformanceComparison() {
        int subscriptionCount = 10_000;

        for (int i = 0; i < subscriptionCount; i++) {
            String topic = "sensor/room" + (i % 100) + "/device" + (i % 50) + "/metric" + (i % 20);
            trie.addSubscription(ParsedTopic.parse(topic).getLevels(), "client" + i);
        }

        trie.addSubscription(ParsedTopic.parse("sensor/+/device10/+").getLevels(), "wildcardClient1");
        trie.addSubscription(ParsedTopic.parse("sensor/room5/#").getLevels(), "wildcardClient2");
        trie.addSubscription(ParsedTopic.parse("sensor/room5/device10/metric3").getLevels(), "exactClient");

        String publishTopic = "sensor/room5/device10/metric3";

        long startTime = System.nanoTime();
        Set<String> trieMatches = trie.findMatches(ParsedTopic.parse(publishTopic).getLevels());
        long trieTime = System.nanoTime() - startTime;

        System.out.printf("Trie Performance: Found %d matches in %.3f ms%n",
                trieMatches.size(), trieTime / 1_000_000.0);

        assertFalse(trieMatches.isEmpty(), "Should find at least the wildcard and exact subscriptions");
        assertTrue(trieMatches.contains("wildcardClient1"));
        assertTrue(trieMatches.contains("wildcardClient2"));
        assertTrue(trieMatches.contains("exactClient"));

        assertTrue(trieTime / 1_000_000.0 < 1.0,
                "Trie lookup should be < 1ms for 10K subscriptions");
    }

    @Test
    void testConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        int subscriptionsPerThread = 100;

        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                for (int i = 0; i < subscriptionsPerThread; i++) {
                    String topic = "topic" + threadId + "/" + i;
                    trie.addSubscription(ParsedTopic.parse(topic).getLevels(), "client" + threadId + "_" + i);
                }
            });
            threads[t].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(threadCount * subscriptionsPerThread, trie.getTotalSubscriptionCount());
    }
}
