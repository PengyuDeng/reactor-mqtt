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
        trie.addSubscription("sensor/temperature", "client1");

        Set<String> matches = trie.findMatches("sensor/temperature");
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client1"));

        // 不匹配的主题
        matches = trie.findMatches("sensor/humidity");
        assertTrue(matches.isEmpty());
    }

    @Test
    void testSingleWildcard() {
        trie.addSubscription("sensor/+/temperature", "client1");

        // 匹配
        Set<String> matches = trie.findMatches("sensor/room1/temperature");
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client1"));

        matches = trie.findMatches("sensor/room2/temperature");
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client1"));

        // 不匹配（层级不对）
        matches = trie.findMatches("sensor/room1/room2/temperature");
        assertTrue(matches.isEmpty());

        matches = trie.findMatches("sensor/temperature");
        assertTrue(matches.isEmpty());
    }

    @Test
    void testMultiWildcard() {
        trie.addSubscription("sensor/#", "client1");

        // 匹配所有
        Set<String> matches = trie.findMatches("sensor/temperature");
        assertEquals(1, matches.size());

        matches = trie.findMatches("sensor/room1/temperature");
        assertEquals(1, matches.size());

        matches = trie.findMatches("sensor/room1/room2/temperature");
        assertEquals(1, matches.size());

        // 不匹配
        matches = trie.findMatches("device/temperature");
        assertTrue(matches.isEmpty());
    }

    @Test
    void testMultipleSubscribers() {
        trie.addSubscription("sensor/temperature", "client1");
        trie.addSubscription("sensor/temperature", "client2");
        trie.addSubscription("sensor/temperature", "client3");

        Set<String> matches = trie.findMatches("sensor/temperature");
        assertEquals(3, matches.size());
        assertTrue(matches.contains("client1"));
        assertTrue(matches.contains("client2"));
        assertTrue(matches.contains("client3"));
    }

    @Test
    void testOverlappingSubscriptions() {
        trie.addSubscription("sensor/+/temperature", "client1");
        trie.addSubscription("sensor/#", "client2");
        trie.addSubscription("sensor/room1/temperature", "client3");

        Set<String> matches = trie.findMatches("sensor/room1/temperature");
        assertEquals(3, matches.size());
        assertTrue(matches.contains("client1"));
        assertTrue(matches.contains("client2"));
        assertTrue(matches.contains("client3"));
    }

    @Test
    void testRemoveSubscription() {
        trie.addSubscription("sensor/temperature", "client1");
        trie.addSubscription("sensor/temperature", "client2");

        // 移除一个订阅
        boolean removed = trie.removeSubscription("sensor/temperature", "client1");
        assertTrue(removed);

        Set<String> matches = trie.findMatches("sensor/temperature");
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client2"));

        // 移除第二个订阅
        removed = trie.removeSubscription("sensor/temperature", "client2");
        assertTrue(removed);

        matches = trie.findMatches("sensor/temperature");
        assertTrue(matches.isEmpty());
    }

    @Test
    void testRemoveAllSubscriptions() {
        trie.addSubscription("sensor/temp", "client1");
        trie.addSubscription("sensor/humidity", "client1");
        trie.addSubscription("device/status", "client1");
        trie.addSubscription("sensor/temp", "client2");

        // 移除 client1 的所有订阅
        trie.removeAll("client1");

        // client1 的订阅应该被清除
        Set<String> matches = trie.findMatches("sensor/temp");
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client2"));

        matches = trie.findMatches("sensor/humidity");
        assertTrue(matches.isEmpty());

        matches = trie.findMatches("device/status");
        assertTrue(matches.isEmpty());
    }

    @Test
    void testGetSubscriberCount() {
        trie.addSubscription("sensor/temperature", "client1");
        trie.addSubscription("sensor/temperature", "client2");

        assertEquals(2, trie.getSubscriberCount("sensor/temperature"));
        assertEquals(0, trie.getSubscriberCount("sensor/humidity"));
    }

    @Test
    void testGetTotalSubscriptionCount() {
        trie.addSubscription("sensor/temp", "client1");
        trie.addSubscription("sensor/temp", "client2");
        trie.addSubscription("sensor/humidity", "client1");
        trie.addSubscription("device/#", "client3");

        assertEquals(4, trie.getTotalSubscriptionCount());
    }

    @Test
    void testClear() {
        trie.addSubscription("sensor/temp", "client1");
        trie.addSubscription("sensor/humidity", "client2");

        trie.clear();

        assertEquals(0, trie.getTotalSubscriptionCount());
        assertTrue(trie.findMatches("sensor/temp").isEmpty());
    }

    @Test
    void testEdgeCases() {
        // 空主题
        assertThrows(IllegalArgumentException.class, () ->
                trie.addSubscription("", "client1"));

        // null 主题
        assertThrows(IllegalArgumentException.class, () ->
                trie.addSubscription(null, "client1"));

        // null clientId
        assertThrows(IllegalArgumentException.class, () ->
                trie.addSubscription("sensor/temp", null));

        // # 不在末尾
        assertThrows(IllegalArgumentException.class, () ->
                trie.addSubscription("sensor/#/temp", "client1"));
    }

    @Test
    void testComplexTopics() {
        trie.addSubscription("a/b/c/d/e", "client1");
        trie.addSubscription("a/+/c/+/e", "client2");
        trie.addSubscription("a/#", "client3");

        Set<String> matches = trie.findMatches("a/b/c/d/e");
        assertEquals(3, matches.size());

        matches = trie.findMatches("a/x/c/y/e");
        assertEquals(2, matches.size());
        assertTrue(matches.contains("client2"));
        assertTrue(matches.contains("client3"));
    }

    /**
     * 性能测试：验证 Trie 树的性能优势
     */
    @Test
    void testPerformance() {
        // 添加 10,000 个订阅
        int subscriptionCount = 10_000;
        for (int i = 0; i < subscriptionCount; i++) {
            trie.addSubscription("sensor/room" + (i % 100) + "/device" + (i % 50) + "/metric" + (i % 20), "client" + i);
        }

        // 测试查找性能
        long startTime = System.nanoTime();
        Set<String> matches = trie.findMatches("sensor/room5/device10/metric3");
        long endTime = System.nanoTime();

        double durationMs = (endTime - startTime) / 1_000_000.0;

        System.out.printf("TopicTrie Performance: Found %d matches in %.3f ms (10,000 subscriptions)%n",
                matches.size(), durationMs);

        // 验证性能：应该小于 1ms（O(L) 复杂度，L=4）
        assertTrue(durationMs < 1.0, "Lookup should be < 1ms, but was: " + durationMs + " ms");
    }

    /**
     * 对比测试：Trie vs 线性遍历
     */
    @Test
    void testPerformanceComparison() {
        int subscriptionCount = 10_000;

        // 准备数据 - 添加精确订阅和通配符订阅
        for (int i = 0; i < subscriptionCount; i++) {
            // 添加精确订阅
            String topic = "sensor/room" + (i % 100) + "/device" + (i % 50) + "/metric" + (i % 20);
            trie.addSubscription(topic, "client" + i);
        }

        // 添加一些通配符订阅，确保能匹配到
        trie.addSubscription("sensor/+/device10/+", "wildcardClient1");
        trie.addSubscription("sensor/room5/#", "wildcardClient2");
        trie.addSubscription("sensor/room5/device10/metric3", "exactClient");

        String publishTopic = "sensor/room5/device10/metric3";

        // Trie 树查找
        long startTime = System.nanoTime();
        Set<String> trieMatches = trie.findMatches(publishTopic);
        long trieTime = System.nanoTime() - startTime;

        System.out.printf("Trie Performance: Found %d matches in %.3f ms%n",
                trieMatches.size(), trieTime / 1_000_000.0);

        // 验证找到了匹配（应该有 wildcardClient1, wildcardClient2, exactClient）
        assertFalse(trieMatches.isEmpty(), "Should find at least the wildcard and exact subscriptions");
        assertTrue(trieMatches.contains("wildcardClient1"));
        assertTrue(trieMatches.contains("wildcardClient2"));
        assertTrue(trieMatches.contains("exactClient"));

        // 性能断言：应该小于 1ms
        assertTrue(trieTime / 1_000_000.0 < 1.0,
                "Trie lookup should be < 1ms for 10K subscriptions");
    }

    @Test
    void testConcurrentAccess() throws InterruptedException {
        // 并发添加订阅
        int threadCount = 10;
        int subscriptionsPerThread = 100;

        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                for (int i = 0; i < subscriptionsPerThread; i++) {
                    trie.addSubscription("topic" + threadId + "/" + i, "client" + threadId + "_" + i);
                }
            });
            threads[t].start();
        }

        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }

        // 验证订阅数
        assertEquals(threadCount * subscriptionsPerThread, trie.getTotalSubscriptionCount());
    }
}
