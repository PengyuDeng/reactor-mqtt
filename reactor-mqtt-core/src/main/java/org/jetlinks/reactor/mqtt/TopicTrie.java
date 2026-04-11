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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.jetlinks.reactor.mqtt.MqttConstants.Topic.MULTI_WILDCARD;
import static org.jetlinks.reactor.mqtt.MqttConstants.Topic.SINGLE_WILDCARD;

/**
 * 通用的 MQTT 主题 Trie 树实现
 *
 * <p>支持 MQTT 主题通配符的高性能匹配引擎，时间复杂度 O(L)，L = 主题层级数。</p>
 *
 * <h3>支持的通配符</h3>
 * <ul>
 *   <li>{@code +} - 单层通配符，匹配一个层级</li>
 *   <li>{@code #} - 多层通配符，匹配零个或多个层级（必须在末尾）</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>Broker消息路由</b>: 存储订阅关系 (topic → Set&lt;clientId&gt;)，快速查找匹配的订阅者</li>
 *   <li><b>Client订阅管理</b>: 存储订阅处理器 (topic → List&lt;Handler&gt;)，快速查找匹配的处理器</li>
 * </ul>
 *
 * <h3>性能特性</h3>
 * <pre>
 * 操作              时间复杂度    说明
 * ─────────────────────────────────────────
 * addSubscription   O(L)         L = 主题层级数
 * findMatches       O(L)         与订阅总数N无关！
 * removeSubscription O(L)        自动清理空节点
 * </pre>
 *
 * <h3>线程安全</h3>
 * <p>使用 ConcurrentHashMap 和 COW 集合，支持高并发读写。</p>
 *
 * @param <T> 订阅数据类型（如 String clientId，或 Handler 对象）
 * @author PengyuDeng
 */
public class TopicTrie<T> {

    private final TrieNode<T> root;
    private final Supplier<Collection<T>> collectionFactory;
    private final ReentrantLock writeLock = new ReentrantLock();

    private static final ThreadLocal<MatchResultPool> RESULT_POOL = ThreadLocal.withInitial(MatchResultPool::new);

    /**
     * 创建一个新的 TopicTrie，使用 CopyOnWriteArraySet 存储订阅者（适合读多写少）
     */
    public TopicTrie() {
        this(CopyOnWriteArraySet::new);
    }

    /**
     * 创建一个新的 TopicTrie，自定义集合类型
     *
     * @param collectionFactory 集合工厂，用于创建存储订阅者的集合
     */
    public TopicTrie(Supplier<Collection<T>> collectionFactory) {
        this.root = new TrieNode<>(collectionFactory);
        this.collectionFactory = collectionFactory;
    }

    /**
     * 添加订阅
     *
     * @param levels       主题层级数组（请勿在调用后修改）
     * @param subscription 订阅数据
     * @throws IllegalArgumentException 如果主题格式非法
     */
    public void addSubscription(String[] levels, T subscription) {
        if (levels == null || levels.length == 0) {
            throw new IllegalArgumentException("Topic levels must not be null or empty");
        }
        if (subscription == null) {
            throw new IllegalArgumentException("Subscription must not be null");
        }

        if (levels.length == 1 && levels[0].isEmpty()) {
            throw new IllegalArgumentException("Topic must not be empty");
        }

        writeLock.lock();
        try {
            TrieNode<T> current = root;

            for (int i = 0; i < levels.length; i++) {
                String level = levels[i];

                if (MULTI_WILDCARD.equals(level)) {
                    if (i != levels.length - 1) {
                        throw new IllegalArgumentException("# wildcard must be the last level");
                    }
                    TrieNode<T> hashWildcard = current.hashWildcard();
                    if (hashWildcard == null) {
                        hashWildcard = new TrieNode<>(collectionFactory);
                        current.setHashWildcard(hashWildcard);
                    }
                    current = hashWildcard;
                    break;
                } else if (SINGLE_WILDCARD.equals(level)) {
                    TrieNode<T> plusWildcard = current.plusWildcard();
                    if (plusWildcard == null) {
                        plusWildcard = new TrieNode<>(collectionFactory);
                        current.setPlusWildcard(plusWildcard);
                    }
                    current = plusWildcard;
                } else {
                    current = current.getOrCreateChildren().computeIfAbsent(level, k -> new TrieNode<>(collectionFactory));
                }
            }

            current.subscriptions.add(subscription);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 移除订阅
     *
     * @param levels       主题层级数组
     * @param subscription 订阅数据
     * @return true 如果成功移除
     */
    public boolean removeSubscription(String[] levels, T subscription) {
        if (levels == null || levels.length == 0 || subscription == null) {
            return false;
        }

        writeLock.lock();
        try {
            boolean[] removed = new boolean[1];
            removeSubscriptionRecursive(root, levels, 0, subscription, removed);
            return removed[0];
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 递归移除订阅，并清理空节点
     */
    private boolean removeSubscriptionRecursive(TrieNode<T> node, String[] levels, int depth,
                                                T subscription, boolean[] removed) {
        if (depth == levels.length) {
            if (node.subscriptions.remove(subscription)) {
                removed[0] = true;
            }
            return node.isEmpty();
        }

        String level = levels[depth];

        if (MULTI_WILDCARD.equals(level)) {
            TrieNode<T> hashWildcard = node.hashWildcard();
            if (hashWildcard != null) {
                if (hashWildcard.subscriptions.remove(subscription)) {
                    removed[0] = true;
                }
                if (hashWildcard.isEmpty()) {
                    node.setHashWildcard(null);
                }
            }
        } else if (SINGLE_WILDCARD.equals(level)) {
            TrieNode<T> plusWildcard = node.plusWildcard();
            if (plusWildcard != null) {
                boolean shouldDelete = removeSubscriptionRecursive(plusWildcard, levels, depth + 1, subscription, removed);
                if (shouldDelete) {
                    node.setPlusWildcard(null);
                }
            }
        } else {
            Map<String, TrieNode<T>> children = node.children();
            TrieNode<T> child = children != null ? children.get(level) : null;
            if (child != null) {
                boolean shouldDelete = removeSubscriptionRecursive(child, levels, depth + 1, subscription, removed);
                if (shouldDelete) {
                    children.remove(level);
                }
            }
        }

        return node.isEmpty();
    }

    /**
     * 查找匹配指定发布主题的所有订阅
     *
     * <p>使用 ThreadLocal 集合复用，减少 GC 压力</p>
     *
     * @param levels 发布主题的层级数组
     * @return 匹配的订阅数据集合（只读）
     */
    public Set<T> findMatches(String[] levels) {
        if (levels == null || levels.length == 0) {
            return Collections.emptySet();
        }

        // 从 ThreadLocal 池获取可复用的集合
        MatchResultPool pool = RESULT_POOL.get();
        Set<T> workingSet = pool.acquire();

        try {
            workingSet.clear();  // 清空上次的结果
            findMatchesRecursive(root, levels, 0, workingSet);

            // 返回不可变副本（调用方可以安全持有）
            return workingSet.isEmpty()
                    ? Collections.emptySet()
                    : Set.copyOf(workingSet);
        } finally {
            // 归还到池中（不清空，下次使用时再清空）
            pool.release(workingSet);
        }
    }

    /**
     * 递归查找匹配的订阅
     */
    private void findMatchesRecursive(TrieNode<T> node, String[] levels, int depth, Set<T> result) {
        if (depth == levels.length) {
            result.addAll(node.subscriptions);
            TrieNode<T> hashWildcard = node.hashWildcard();
            if (hashWildcard != null) {
                result.addAll(hashWildcard.subscriptions);
            }
            return;
        }

        TrieNode<T> hashWildcard = node.hashWildcard();
        if (hashWildcard != null) {
            result.addAll(hashWildcard.subscriptions);
        }

        String level = levels[depth];

        Map<String, TrieNode<T>> children = node.children();
        TrieNode<T> exactNode = children != null ? children.get(level) : null;
        if (exactNode != null) {
            findMatchesRecursive(exactNode, levels, depth + 1, result);
        }

        TrieNode<T> plusWildcard = node.plusWildcard();
        if (plusWildcard != null) {
            findMatchesRecursive(plusWildcard, levels, depth + 1, result);
        }
    }

    /**
     * 移除某个订阅数据的所有订阅（遍历整个树）
     *
     * @param subscription 订阅数据
     */
    public void removeAll(T subscription) {
        if (subscription == null) {
            return;
        }
        writeLock.lock();
        try {
            removeAllRecursive(root, subscription);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 递归移除订阅数据的所有订阅
     */
    private void removeAllRecursive(TrieNode<T> node, T subscription) {
        node.subscriptions.remove(subscription);

        Map<String, TrieNode<T>> children = node.children();
        if (children != null) {
            for (TrieNode<T> child : children.values()) {
                removeAllRecursive(child, subscription);
            }
        }

        TrieNode<T> plusWildcard = node.plusWildcard();
        if (plusWildcard != null) {
            removeAllRecursive(plusWildcard, subscription);
        }

        TrieNode<T> hashWildcard = node.hashWildcard();
        if (hashWildcard != null) {
            removeAllRecursive(hashWildcard, subscription);
        }
    }

    /**
     * 获取指定主题的订阅者数量
     *
     * @param levels 订阅主题的层级数组
     * @return 订阅者数量
     */
    public int getSubscriberCount(String[] levels) {
        if (levels == null || levels.length == 0) {
            return 0;
        }

        TrieNode<T> current = root;

        for (String level : levels) {
            if (MULTI_WILDCARD.equals(level)) {
                current = current.hashWildcard();
                break;
            } else if (SINGLE_WILDCARD.equals(level)) {
                current = current.plusWildcard();
            } else {
                Map<String, TrieNode<T>> children = current.children();
                current = children != null ? children.get(level) : null;
            }

            if (current == null) {
                return 0;
            }
        }

        return current.subscriptions.size();
    }

    /**
     * 获取总订阅数
     *
     * @return 订阅总数
     */
    public int getTotalSubscriptionCount() {
        return countSubscriptionsRecursive(root);
    }

    private int countSubscriptionsRecursive(TrieNode<T> node) {
        int count = node.subscriptions.size();

        Map<String, TrieNode<T>> children = node.children();
        if (children != null) {
            for (TrieNode<T> child : children.values()) {
                count += countSubscriptionsRecursive(child);
            }
        }

        TrieNode<T> plusWildcard = node.plusWildcard();
        if (plusWildcard != null) {
            count += countSubscriptionsRecursive(plusWildcard);
        }

        TrieNode<T> hashWildcard = node.hashWildcard();
        if (hashWildcard != null) {
            count += countSubscriptionsRecursive(hashWildcard);
        }

        return count;
    }

    /**
     * 清空所有订阅
     */
    public void clear() {
        writeLock.lock();
        try {
            Map<String, TrieNode<T>> children = root.children();
            if (children != null) {
                children.clear();
            }
            root.setChildren(null);
            root.setPlusWildcard(null);
            root.setHashWildcard(null);
            root.subscriptions.clear();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Trie 树节点 - 极致内存优化版本
     *
     * <h3>内存优化策略</h3>
     * <ul>
     *   <li>懒加载 children Map：只有在需要时才创建</li>
     *   <li>使用 computeIfAbsent 避免不必要的 Map 创建</li>
     *   <li>空节点自动清理，避免内存泄漏</li>
     * </ul>
     */
    private static class TrieNode<T> {
        private static final VarHandle CHILDREN;
        private static final VarHandle PLUS_WILDCARD;
        private static final VarHandle HASH_WILDCARD;

        static {
            try {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                CHILDREN = lookup.findVarHandle(TrieNode.class, "children", Map.class);
                PLUS_WILDCARD = lookup.findVarHandle(TrieNode.class, "plusWildcard", TrieNode.class);
                HASH_WILDCARD = lookup.findVarHandle(TrieNode.class, "hashWildcard", TrieNode.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        // 懒加载：只有在有子节点时才创建 Map
        @SuppressWarnings("unused")
        private Map<String, TrieNode<T>> children;
        @SuppressWarnings("unused")
        private TrieNode<T> plusWildcard;
        @SuppressWarnings("unused")
        private TrieNode<T> hashWildcard;
        final Collection<T> subscriptions;

        TrieNode(Supplier<Collection<T>> collectionFactory) {
            this.subscriptions = collectionFactory.get();
        }

        @SuppressWarnings("unchecked")
        Map<String, TrieNode<T>> children() {
            return (Map<String, TrieNode<T>>) CHILDREN.getAcquire(this);
        }

        Map<String, TrieNode<T>> getOrCreateChildren() {
            Map<String, TrieNode<T>> children = children();
            if (children == null) {
                children = new ConcurrentHashMap<>();
                setChildren(children);
            }
            return children;
        }

        void setChildren(Map<String, TrieNode<T>> children) {
            CHILDREN.setRelease(this, children);
        }

        @SuppressWarnings("unchecked")
        TrieNode<T> plusWildcard() {
            return (TrieNode<T>) PLUS_WILDCARD.getAcquire(this);
        }

        void setPlusWildcard(TrieNode<T> plusWildcard) {
            PLUS_WILDCARD.setRelease(this, plusWildcard);
        }

        @SuppressWarnings("unchecked")
        TrieNode<T> hashWildcard() {
            return (TrieNode<T>) HASH_WILDCARD.getAcquire(this);
        }

        void setHashWildcard(TrieNode<T> hashWildcard) {
            HASH_WILDCARD.setRelease(this, hashWildcard);
        }

        boolean isEmpty() {
            Map<String, TrieNode<T>> children = children();
            return subscriptions.isEmpty() &&
                    (children == null || children.isEmpty()) &&
                    plusWildcard() == null &&
                    hashWildcard() == null;
        }
    }

    /**
     * ThreadLocal 结果集池
     */
    private static class MatchResultPool {
        private final Set<?> reusableSet = new HashSet<>();
        private boolean inUse = false;

        @SuppressWarnings("unchecked")
        <T> Set<T> acquire() {
            if (inUse) {
                return new HashSet<>();
            }
            inUse = true;
            return (Set<T>) reusableSet;
        }

        void release(Set<?> set) {
            if (set == reusableSet) {
                inUse = false;
            }
        }
    }
}
