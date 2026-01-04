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
import org.jetlinks.reactor.mqtt.TopicMatcher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * 默认订阅管理器实现
 *
 * @author PengyuDeng
 */
public class DefaultSubscriptionManager implements SubscriptionManager {

    private static final Logger log = Logger.getLogger(DefaultSubscriptionManager.class.getName());

    private final Map<String, SubscriptionHandlers> subscriptions = new ConcurrentHashMap<>();

    @Override
    public Disposable subscribe(ClientConnection connection,
                                CharSequence topic,
                                MqttQoS qos,
                                Function<ClientReceivedPublish, Mono<Void>> handler) {
        String topicStr = topic.toString();

        // 获取或创建订阅处理器容器
        SubscriptionHandlers handlers = subscriptions.computeIfAbsent(
                topicStr,
                k -> new SubscriptionHandlers(topicStr, qos, connection)
        );

        // 添加处理器并返回 Disposable
        return handlers.addHandler(handler);
    }

    @Override
    public Mono<Void> handleMessage(ClientReceivedPublish publishing) {
        String topic = publishing.getTopic();

        // 使用 Flux 并行处理所有匹配的订阅，每个订阅独立隔离错误
        return Flux.fromIterable(subscriptions.entrySet())
                   .filter(entry -> TopicMatcher.matches(entry.getKey(), topic))
                   .flatMap(entry -> entry.getValue().handle(publishing))
                   .then();
    }

    @Override
    public Iterable<SubscriptionInfo> getSubscriptions() {
        return subscriptions.values()
                            .stream()
                            .map(h -> (SubscriptionInfo) new SubscriptionInfoImpl(h.topic, h.qos))
                            .collect(Collectors.toList());
    }

    @Override
    public void clear() {
        subscriptions.values().forEach(SubscriptionHandlers::dispose);
        subscriptions.clear();
    }

    /**
     * 订阅信息实现
     */
    private record SubscriptionInfoImpl(String topic, MqttQoS qos) implements SubscriptionInfo {
    }

    /**
     * 订阅处理器容器，支持同一主题多个处理器
     */
    private class SubscriptionHandlers {
        private final String topic;
        private final MqttQoS qos;
        private final ClientConnection connection;
        private final List<Function<ClientReceivedPublish, Mono<Void>>> handlers = new CopyOnWriteArrayList<>();
        private volatile boolean subscribed = false;
        private final Object subscribeLock = new Object();

        SubscriptionHandlers(String topic, MqttQoS qos, ClientConnection connection) {
            this.topic = topic;
            this.qos = qos;
            this.connection = connection;
        }

        /**
         * 添加处理器
         *
         * @param handler 消息处理器
         * @return Disposable 用于移除此处理器
         */
        Disposable addHandler(Function<ClientReceivedPublish, Mono<Void>> handler) {
            handlers.add(handler);

            // 第一个处理器时执行实际订阅
            if (!subscribed) {
//                synchronized (subscribeLock) {
//                    if (!subscribed) {
//                        connection.subscribe(topic, qos).subscribe(
//                                v -> {
//                                },
//                                error -> {
//                                    // 只记录非超时错误，连接关闭时的超时是正常的
//                                    if (!(error instanceof java.util.concurrent.TimeoutException)) {
//                                        log.log(Level.WARNING,
//                                                "Failed to subscribe to topic: " + topic,
//                                                error);
//                                    }
//                                }
//                        );
//                        subscribed = true;
//                    }
//                }
            }

            // 返回 Disposable 用于移除此处理器
            return () -> {
                handlers.remove(handler);

                // 如果没有处理器了，取消订阅并移除容器
                if (handlers.isEmpty()) {
                    dispose();
                    subscriptions.remove(topic);
                }
            };
        }

        /**
         * 处理消息，调用所有处理器
         */
        Mono<Void> handle(ClientReceivedPublish publishing) {
            return Flux.fromIterable(handlers)
                       .flatMap(h -> h.apply(publishing)
                                      .onErrorResume(error -> {
                                          log.log(Level.WARNING,
                                                  String.format("Handler error for topic [%s]: %s",
                                                                topic, error.getMessage()),
                                                  error);
                                          return Mono.empty();
                                      }))
                       .then();
        }

        /**
         * 清理资源
         */
        void dispose() {
            if (subscribed && connection.isAlive()) {
                connection.unsubscribe(topic).subscribe(
                        v -> {
                        },
                        error -> {
                            // 只记录非超时错误，连接关闭时的超时是正常的
                            if (!(error instanceof java.util.concurrent.TimeoutException)) {
                                log.log(Level.WARNING,
                                        "Failed to unsubscribe from topic: " + topic,
                                        error);
                            }
                        }
                );
            }
            subscribed = false;
            handlers.clear();
        }
    }
}
