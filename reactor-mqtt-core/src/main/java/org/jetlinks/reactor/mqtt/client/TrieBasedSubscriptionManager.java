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
package org.jetlinks.reactor.mqtt.client;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.jetlinks.reactor.mqtt.Topic;
import org.jetlinks.reactor.mqtt.TopicTrie;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 Trie 树的高性能订阅管理器
 *
 * <p>使用通用 TopicTrie 优化主题匹配性能，特别适合大量订阅的场景。</p>
 * <p>时间复杂度：O(L) 其中 L 是主题层级数，而不是 O(N) 其中 N 是订阅数量。</p>
 *
 * @author PengyuDeng
 */
class TrieBasedSubscriptionManager implements SubscriptionManager {

    private static final Logger log = Logger.getLogger(TrieBasedSubscriptionManager.class.getName());

    // 使用通用 TopicTrie 管理订阅，存储 SubscriptionHandlers 对象
    private final TopicTrie<SubscriptionHandlers> trie = new TopicTrie<>();
    // 用于快速查找和清理
    private final Map<String, SubscriptionHandlers> subscriptionsMap = new ConcurrentHashMap<>();

    @Override
    public Disposable subscribe(ClientConnection connection,
                                CharSequence topic,
                                MqttQoS qos,
                                Function<ClientReceivedPublish, Mono<Void>> handler) {
        String topicStr = topic.toString();

        // 获取或创建订阅处理器容器
        String[] levels = Topic.of(topicStr).getLevels();
        SubscriptionHandlers handlers = subscriptionsMap.computeIfAbsent(
                topicStr,
                k -> {
                    TrieBasedSubscriptionHandlers h = new TrieBasedSubscriptionHandlers(topicStr, qos, connection);
                    // 将订阅添加到通用 TopicTrie
                    trie.addSubscription(levels, h);
                    return h;
                }
        );

        // 添加处理器并返回 Disposable
        return handlers.addHandler(handler, () -> {
            // 当最后一个处理器被移除时，从 Trie 和 Map 中删除
            AtomicBoolean removed = new AtomicBoolean(false);
            subscriptionsMap.compute(topicStr, (key, current) -> {
                if (current == handlers) {
                    removed.set(true);
                    return null;
                }
                return current;
            });
            if (removed.get()) {
                trie.removeSubscription(levels, handlers);
            }
        });
    }

    @Override
    public Mono<Void> handleMessage(ClientReceivedPublish publishing) {
        String[] topicLevels = publishing.topic().getLevels();

        return ReactiveTaskSupport.whenAll(
                trie.findMatches(topicLevels),
                matched -> matched.handle(publishing)
        );
    }

    @Override
    public Iterable<SubscriptionInfo> getSubscriptions() {
        return subscriptionsMap.values()
                               .stream()
                               .map(DefaultSubscriptionInfo::of)
                               .toList();
    }

    @Override
    public void clear() {
        subscriptionsMap.values().forEach(SubscriptionHandlers::dispose);
        subscriptionsMap.clear();
        trie.clear();
    }

    /**
     * 订阅处理器容器，支持同一主题多个处理器
     */
    private static class TrieBasedSubscriptionHandlers implements SubscriptionHandlers {

        private static final VarHandle SUBSCRIBED;

        static {
            try {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                SUBSCRIBED = lookup.findVarHandle(TrieBasedSubscriptionHandlers.class, "subscribed", boolean.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final String topic;

        private final MqttQoS qos;

        private final ClientConnection connection;

        private final List<Function<ClientReceivedPublish, Mono<Void>>> handlers = new CopyOnWriteArrayList<>();

        @SuppressWarnings("unused")
        private volatile boolean subscribed = false;

        TrieBasedSubscriptionHandlers(String topic, MqttQoS qos, ClientConnection connection) {
            this.topic = topic;
            this.qos = qos;
            this.connection = connection;
        }

        @Override
        public String getTopic() {
            return topic;
        }

        @Override
        public MqttQoS getQos() {
            return qos;
        }

        @Override
        public ClientConnection getConnection() {
            return connection;
        }

        @Override
        public boolean isSubscribed() {
            return (boolean) SUBSCRIBED.get(this);
        }

        @Override
        public Disposable addHandler(Function<ClientReceivedPublish, Mono<Void>> handler, Runnable onLastRemoved) {
            handlers.add(handler);

            // 第一个处理器时执行实际订阅 - 使用CAS保证只执行一次
            if (SUBSCRIBED.compareAndSet(this, false, true)) {
                // 首次订阅，发送 SUBSCRIBE 消息到服务器
                if (connection instanceof DefaultClientConnection c) {
                    c.runManagedTask(
                            c.doSubscribe(topic, qos),
                            () -> log.log(Level.FINE, () -> "Successfully subscribed to topic: " + topic),
                            error -> log.log(Level.WARNING, error, () -> "Failed to subscribe to topic: " + topic)
                    );
                }
            }

            // 返回 Disposable 用于移除此处理器
            return () -> {
                handlers.remove(handler);

                // 如果没有处理器了，取消订阅并触发清理回调
                if (handlers.isEmpty()) {
                    dispose();
                    if (onLastRemoved != null) {
                        onLastRemoved.run();
                    }
                }
            };
        }

        @Override
        public Mono<Void> handle(ClientReceivedPublish publishing) {
            return HandlerDispatchSupport.dispatch(handlers, publishing, topic, log);
        }

        @Override
        public void dispose() {
            if ((boolean) SUBSCRIBED.get(this) && connection.isAlive() && connection instanceof DefaultClientConnection c) {
                c.runManagedTask(
                        connection.unsubscribe(topic),
                        null,
                        error -> {
                            // 只记录非超时错误
                            if (!(error instanceof java.util.concurrent.TimeoutException)) {
                                log.log(Level.WARNING, error, () -> "Failed to unsubscribe from topic: " + topic);
                            }
                        }
                );
            }
            SUBSCRIBED.set(this, false);
            handlers.clear();
        }

        /**
         * 基于主题的相等性判断
         * <p>
         * 两个 SubscriptionHandlers 如果订阅的主题相同，则被视为相等。
         * 这确保了在 Set 集合中不会出现重复的主题订阅。
         * </p>
         *
         * @param o 要比较的对象
         * @return true 如果主题相同
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TrieBasedSubscriptionHandlers that = (TrieBasedSubscriptionHandlers) o;
            return topic.equals(that.topic);
        }

        /**
         * 基于主题的哈希码
         * <p>
         * 与 equals() 保持一致，只基于 topic 计算哈希值。
         * </p>
         *
         * @return 主题的哈希码
         */
        @Override
        public int hashCode() {
            return topic.hashCode();
        }
    }
}
