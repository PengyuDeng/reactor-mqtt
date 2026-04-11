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
package org.jetlinks.reactor.mqtt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TopicTrie 数组 API 测试
 *
 * @author PengyuDeng
 */
class TopicTrieArrayAPITest {

    private TopicTrie<String> trie;

    @BeforeEach
    void setUp() {
        trie = new TopicTrie<>();
    }

    @Test
    void testAddAndFindWithArrays() {
        String[] levels = {"sensor", "temp", "room1"};
        trie.addSubscription(levels, "client1");

        Set<String> matches = trie.findMatches(levels);
        assertEquals(1, matches.size());
        assertTrue(matches.contains("client1"));
    }

    @Test
    void testWildcardsWithArrays() {
        trie.addSubscription(new String[]{"sensor", "+", "temp"}, "client1");
        trie.addSubscription(new String[]{"sensor", "#"}, "client2");

        Set<String> matches = trie.findMatches(new String[]{"sensor", "room1", "temp"});
        assertEquals(2, matches.size());
        assertTrue(matches.contains("client1"));
        assertTrue(matches.contains("client2"));
    }

    @Test
    void testRemoveWithArrays() {
        String[] levels = {"sensor", "temp"};
        trie.addSubscription(levels, "client1");

        boolean removed = trie.removeSubscription(levels, "client1");
        assertTrue(removed);

        Set<String> matches = trie.findMatches(levels);
        assertTrue(matches.isEmpty());
    }

    @Test
    void testGetSubscriberCountWithArrays() {
        String[] levels = {"sensor", "temp"};
        trie.addSubscription(levels, "client1");
        trie.addSubscription(levels, "client2");

        assertEquals(2, trie.getSubscriberCount(levels));
    }

    @Test
    void testEmptyArrayRejected() {
        assertThrows(IllegalArgumentException.class, () ->
            trie.addSubscription(new String[]{}, "client1"));
    }

    @Test
    void testNullArrayRejected() {
        assertThrows(IllegalArgumentException.class, () ->
            trie.addSubscription((String[]) null, "client1"));
    }

    @Test
    void testHashWildcardMustBeLast() {
        assertThrows(IllegalArgumentException.class, () ->
            trie.addSubscription(new String[]{"sensor", "#", "temp"}, "client1"));
    }

    @Test
    void testMultiLevelWildcard() {
        trie.addSubscription(new String[]{"sensor", "#"}, "client1");

        // 应该匹配任意深度
        assertTrue(trie.findMatches(new String[]{"sensor"}).contains("client1"));
        assertTrue(trie.findMatches(new String[]{"sensor", "temp"}).contains("client1"));
        assertTrue(trie.findMatches(new String[]{"sensor", "temp", "room1"}).contains("client1"));
        assertTrue(trie.findMatches(new String[]{"sensor", "humidity", "room2", "floor1"}).contains("client1"));
    }

    @Test
    void testSingleLevelWildcard() {
        trie.addSubscription(new String[]{"sensor", "+", "temp"}, "client1");

        // 应该匹配单个层级
        assertTrue(trie.findMatches(new String[]{"sensor", "room1", "temp"}).contains("client1"));
        assertTrue(trie.findMatches(new String[]{"sensor", "room2", "temp"}).contains("client1"));

        // 不应该匹配其他层级数
        assertFalse(trie.findMatches(new String[]{"sensor", "temp"}).contains("client1"));
        assertFalse(trie.findMatches(new String[]{"sensor", "room1", "room2", "temp"}).contains("client1"));
    }

    @Test
    void testReadOnlyResultSet() {
        trie.addSubscription(new String[]{"sensor", "temp"}, "client1");

        Set<String> matches = trie.findMatches(new String[]{"sensor", "temp"});

        // 尝试修改返回的集合应该抛出异常
        assertThrows(UnsupportedOperationException.class, () ->
            matches.add("client2"));
    }

    @Test
    void testArrayNotModifiedByTrie() {
        String[] levels = {"sensor", "temp", "room1"};
        String[] originalCopy = levels.clone();

        trie.addSubscription(levels, "client1");
        trie.findMatches(levels);

        // 验证数组内容没有被修改
        assertArrayEquals(originalCopy, levels);
    }

    @Test
    void testRemoveWithArraysCleansUpEmptyNodes() {
        // 添加嵌套订阅
        trie.addSubscription(new String[]{"sensor", "temp", "room1"}, "client1");
        assertEquals(1, trie.getTotalSubscriptionCount());

        // 移除后应该清理空节点
        trie.removeSubscription(new String[]{"sensor", "temp", "room1"}, "client1");
        assertEquals(0, trie.getTotalSubscriptionCount());

        // 验证树结构被清理（通过添加新订阅不会造成内存泄漏）
        for (int i = 0; i < 100; i++) {
            trie.addSubscription(new String[]{"topic", "level" + i}, "client");
            trie.removeSubscription(new String[]{"topic", "level" + i}, "client");
        }
        assertEquals(0, trie.getTotalSubscriptionCount());
    }

    @Test
    void testZeroCopyBehavior() {
        // 验证零拷贝：同一个数组可以被多次使用
        String[] levels = {"sensor", "temp"};

        trie.addSubscription(levels, "client1");
        trie.addSubscription(levels, "client2");

        Set<String> matches = trie.findMatches(levels);
        assertEquals(2, matches.size());

        // 验证数组仍然可用
        assertEquals("sensor", levels[0]);
        assertEquals("temp", levels[1]);
    }

    @Test
    void testUsageWithParsedTopic() {
        // 演示实际使用场景：配合 ParsedTopic 使用
        String topic1 = "sensor/temp";
        String topic2 = "sensor/humidity";

        ParsedTopic parsed1 = ParsedTopic.parse(topic1);
        ParsedTopic parsed2 = ParsedTopic.parse(topic2);

        trie.addSubscription(parsed1.getLevels(), "client1");
        trie.addSubscription(parsed2.getLevels(), "client2");

        Set<String> matches1 = trie.findMatches(parsed1.getLevels());
        assertEquals(1, matches1.size());
        assertTrue(matches1.contains("client1"));

        Set<String> matches2 = trie.findMatches(parsed2.getLevels());
        assertEquals(1, matches2.size());
        assertTrue(matches2.contains("client2"));
    }

    @Test
    void testConcurrentExactNodeCreation() throws InterruptedException {
        int threadCount = 12;
        int subscriptionsPerThread = 80;

        runConcurrentAdds(threadCount, threadId -> {
            for (int i = 0; i < subscriptionsPerThread; i++) {
                trie.addSubscription(new String[]{"sensor", "room" + threadId, "metric" + i},
                                     "client" + threadId + "_" + i);
            }
        });

        assertEquals(threadCount * subscriptionsPerThread, trie.getTotalSubscriptionCount());
    }

    @Test
    void testConcurrentPlusWildcardCreation() throws InterruptedException {
        int threadCount = 16;

        runConcurrentAdds(threadCount, threadId ->
                trie.addSubscription(new String[]{"sensor", "+", "metric"},
                                     "client" + threadId)
        );

        Set<String> matches = trie.findMatches(new String[]{"sensor", "room1", "metric"});
        assertEquals(threadCount, matches.size());
    }

    @Test
    void testConcurrentHashWildcardCreation() throws InterruptedException {
        int threadCount = 16;

        runConcurrentAdds(threadCount, threadId ->
                trie.addSubscription(new String[]{"sensor", "#"},
                                     "client" + threadId)
        );

        Set<String> matches = trie.findMatches(new String[]{"sensor", "room1", "metric"});
        assertEquals(threadCount, matches.size());
    }

    private void runConcurrentAdds(int threadCount, ThrowingIntConsumer action) throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicReference<Throwable> error = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>(threadCount);

        for (int threadId = 0; threadId < threadCount; threadId++) {
            final int currentThreadId = threadId;
            Thread thread = new Thread(() -> {
                try {
                    ready.countDown();
                    start.await();
                    action.accept(currentThreadId);
                } catch (Throwable e) {
                    error.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            });
            threads.add(thread);
            thread.start();
        }

        ready.await();
        start.countDown();
        done.await();

        if (error.get() != null) {
            fail(error.get());
        }
    }

    @FunctionalInterface
    private interface ThrowingIntConsumer {
        void accept(int value) throws Exception;
    }
}
