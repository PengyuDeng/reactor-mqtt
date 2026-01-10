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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

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

    private static final String LEVEL_SEPARATOR = "/";
    private static final String SINGLE_WILDCARD = "+";
    private static final String MULTI_WILDCARD = "#";

    private final TrieNode<T> root;
    private final Supplier<Collection<T>> collectionFactory;

    /**
     * 创建一个新的 TopicTrie，使用 CopyOnWriteArraySet 存储订阅者（适合读多写少）
     */
    public TopicTrie() {
        this(() -> new java.util.concurrent.CopyOnWriteArraySet<>());
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
     * @param topic        订阅主题（可包含通配符 +, #）
     * @param subscription 订阅数据
     * @throws IllegalArgumentException 如果主题格式非法
     */
    public void addSubscription(String topic, T subscription) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Topic must not be null or empty");
        }
        if (subscription == null) {
            throw new IllegalArgumentException("Subscription must not be null");
        }

        String[] levels = topic.split(LEVEL_SEPARATOR);
        TrieNode<T> current = root;

        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];

            if (MULTI_WILDCARD.equals(level)) {
                if (i != levels.length - 1) {
                    throw new IllegalArgumentException("# wildcard must be the last level in topic: " + topic);
                }
                if (current.hashWildcard == null) {
                    current.hashWildcard = new TrieNode<>(collectionFactory);
                }
                current = current.hashWildcard;
                break;
            } else if (SINGLE_WILDCARD.equals(level)) {
                if (current.plusWildcard == null) {
                    current.plusWildcard = new TrieNode<>(collectionFactory);
                }
                current = current.plusWildcard;
            } else {
                current = current.children.computeIfAbsent(level, k -> new TrieNode<>(collectionFactory));
            }
        }

        current.subscriptions.add(subscription);
    }

    /**
     * 移除订阅
     *
     * @param topic        订阅主题
     * @param subscription 订阅数据
     * @return true 如果成功移除
     */
    public boolean removeSubscription(String topic, T subscription) {
        if (topic == null || topic.isEmpty() || subscription == null) {
            return false;
        }

        String[] levels = topic.split(LEVEL_SEPARATOR);
        boolean[] removed = new boolean[1];
        removeSubscriptionRecursive(root, levels, 0, subscription, removed);
        return removed[0];
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
            if (node.hashWildcard != null) {
                if (node.hashWildcard.subscriptions.remove(subscription)) {
                    removed[0] = true;
                }
                if (node.hashWildcard.isEmpty()) {
                    node.hashWildcard = null;
                }
            }
        } else if (SINGLE_WILDCARD.equals(level)) {
            if (node.plusWildcard != null) {
                boolean shouldDelete = removeSubscriptionRecursive(node.plusWildcard, levels, depth + 1, subscription, removed);
                if (shouldDelete) {
                    node.plusWildcard = null;
                }
            }
        } else {
            TrieNode<T> child = node.children.get(level);
            if (child != null) {
                boolean shouldDelete = removeSubscriptionRecursive(child, levels, depth + 1, subscription, removed);
                if (shouldDelete) {
                    node.children.remove(level);
                }
            }
        }

        return node.isEmpty();
    }

    /**
     * 查找匹配指定发布主题的所有订阅
     *
     * @param publishTopic 发布的主题（不含通配符）
     * @return 匹配的订阅数据集合
     */
    public Set<T> findMatches(String publishTopic) {
        if (publishTopic == null || publishTopic.isEmpty()) {
            return Collections.emptySet();
        }

        String[] levels = publishTopic.split(LEVEL_SEPARATOR);
        Set<T> result = new HashSet<>();
        findMatchesRecursive(root, levels, 0, result);
        return result;
    }

    /**
     * 递归查找匹配的订阅
     */
    private void findMatchesRecursive(TrieNode<T> node, String[] levels, int depth, Set<T> result) {
        if (depth == levels.length) {
            result.addAll(node.subscriptions);
            if (node.hashWildcard != null) {
                result.addAll(node.hashWildcard.subscriptions);
            }
            return;
        }

        if (node.hashWildcard != null) {
            result.addAll(node.hashWildcard.subscriptions);
        }

        String level = levels[depth];

        TrieNode<T> exactNode = node.children.get(level);
        if (exactNode != null) {
            findMatchesRecursive(exactNode, levels, depth + 1, result);
        }

        if (node.plusWildcard != null) {
            findMatchesRecursive(node.plusWildcard, levels, depth + 1, result);
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
        removeAllRecursive(root, subscription);
    }

    /**
     * 递归移除订阅数据的所有订阅
     */
    private void removeAllRecursive(TrieNode<T> node, T subscription) {
        node.subscriptions.remove(subscription);

        for (TrieNode<T> child : node.children.values()) {
            removeAllRecursive(child, subscription);
        }

        if (node.plusWildcard != null) {
            removeAllRecursive(node.plusWildcard, subscription);
        }

        if (node.hashWildcard != null) {
            removeAllRecursive(node.hashWildcard, subscription);
        }
    }

    /**
     * 获取指定主题的订阅者数量
     *
     * @param topic 订阅主题
     * @return 订阅者数量
     */
    public int getSubscriberCount(String topic) {
        if (topic == null || topic.isEmpty()) {
            return 0;
        }

        String[] levels = topic.split(LEVEL_SEPARATOR);
        TrieNode<T> current = root;

        for (String level : levels) {
            if (MULTI_WILDCARD.equals(level)) {
                current = current.hashWildcard;
                break;
            } else if (SINGLE_WILDCARD.equals(level)) {
                current = current.plusWildcard;
            } else {
                current = current.children.get(level);
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

        for (TrieNode<T> child : node.children.values()) {
            count += countSubscriptionsRecursive(child);
        }

        if (node.plusWildcard != null) {
            count += countSubscriptionsRecursive(node.plusWildcard);
        }

        if (node.hashWildcard != null) {
            count += countSubscriptionsRecursive(node.hashWildcard);
        }

        return count;
    }

    /**
     * 清空所有订阅
     */
    public void clear() {
        root.children.clear();
        root.plusWildcard = null;
        root.hashWildcard = null;
        root.subscriptions.clear();
    }

    /**
     * Trie 树节点
     */
    private static class TrieNode<T> {
        final Map<String, TrieNode<T>> children = new ConcurrentHashMap<>();
        TrieNode<T> plusWildcard;
        TrieNode<T> hashWildcard;
        final Collection<T> subscriptions;

        TrieNode(Supplier<Collection<T>> collectionFactory) {
            this.subscriptions = collectionFactory.get();
        }

        boolean isEmpty() {
            return subscriptions.isEmpty() &&
                   children.isEmpty() &&
                   plusWildcard == null &&
                   hashWildcard == null;
        }
    }
}
