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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.jetlinks.reactor.mqtt.TopicMatcher;
import org.jetlinks.reactor.mqtt.server.ServerConnection;
import org.jetlinks.reactor.mqtt.server.ServerConnectionListener;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT 消息路由器 - 负责管理客户端连接和消息路由
 *
 * <p>这个类是 MQTT Broker 的核心组件，负责：
 * <ul>
 *   <li>管理所有活跃的客户端连接</li>
 *   <li>维护订阅关系（clientId -> topics）</li>
 *   <li>路由 PUBLISH 消息到订阅的客户端</li>
 *   <li>处理通配符主题匹配</li>
 * </ul>
 * </p>
 *
 * @author PengyuDeng
 */
class BrokerMessageRouter implements ServerConnectionListener {

    private static final Logger log = Logger.getLogger(BrokerMessageRouter.class.getName());

    /**
     * 存储所有活跃的客户端连接: clientId -> ServerConnection
     */
    private final Map<String, ServerConnection> connections = new ConcurrentHashMap<>();

    /**
     * 存储订阅关系: topic -> Set<clientId>
     * 注意：topic 可能包含通配符（订阅时）或具体路径（发布时）
     */
    private final Map<String, Set<String>> subscriptions = new ConcurrentHashMap<>();

    /**
     * 创建一个新的消息路由器实例
     */
    BrokerMessageRouter() {
    }


    @Override
    public Mono<Void> onConnectionAccepted(String clientId, ServerConnection connection) {
        registerConnection(clientId, connection);
        return Mono.empty();
    }

    @Override
    public Mono<Void> onConnectionClosed(String clientId) {
        unregisterConnection(clientId);
        return Mono.empty();
    }

    @Override
    public Mono<Void> onSubscribe(String clientId, String topic) {
        addSubscription(clientId, topic);
        return Mono.empty();
    }

    @Override
    public Mono<Void> onUnsubscribe(String clientId, String topic) {
        removeSubscription(clientId, topic);
        return Mono.empty();
    }

    @Override
    public Mono<Void> onPublish(String clientId, MqttPublishMessage message) {
        return publish(clientId, message);
    }


    /**
     * 注册一个客户端连接
     *
     * @param clientId   客户端ID
     * @param connection 连接实例
     */
    private void registerConnection(String clientId, ServerConnection connection) {
        if (clientId == null || connection == null) {
            throw new IllegalArgumentException("clientId and connection must not be null");
        }

        ServerConnection old = connections.put(clientId, connection);
        if (old != null && old != connection) {
            log.log(Level.INFO, "Client " + clientId + " reconnected, closing old connection");
            old.close().subscribe();
        }

        log.log(Level.FINE, "Registered connection for client: " + clientId);
    }

    /**
     * 取消注册客户端连接
     *
     * @param clientId 客户端ID
     */
    private void unregisterConnection(String clientId) {
        connections.remove(clientId);
        // 清理该客户端的所有订阅
        subscriptions.values().forEach(clients -> clients.remove(clientId));
        log.log(Level.FINE, "Unregistered connection for client: " + clientId);
    }

    /**
     * 添加订阅
     *
     * @param clientId 客户端ID
     * @param topic    订阅的主题（可能包含通配符）
     */
    private void addSubscription(String clientId, String topic) {
        subscriptions.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(clientId);
        log.log(Level.FINE, "Client " + clientId + " subscribed to: " + topic);
    }

    /**
     * 移除订阅
     *
     * @param clientId 客户端ID
     * @param topic    要取消订阅的主题
     */
    private void removeSubscription(String clientId, String topic) {
        Set<String> clients = subscriptions.get(topic);
        if (clients != null) {
            clients.remove(clientId);
            if (clients.isEmpty()) {
                subscriptions.remove(topic);
            }
        }
        log.log(Level.FINE, "Client " + clientId + " unsubscribed from: " + topic);
    }

    /**
     * 发布消息到所有匹配的订阅者
     *
     * @param publisherClientId 发布者的客户端ID（可能为null）
     * @param topic             消息主题（具体的主题路径，不含通配符）
     * @param payload           消息负载
     * @param qos               QoS级别
     * @param retain            是否保留
     * @return 发布完成的Mono
     */
    private Mono<Void> publish(String publisherClientId, String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        // 查找所有匹配的订阅者
        Set<String> matchedClients = new HashSet<>();

        for (Map.Entry<String, Set<String>> entry : subscriptions.entrySet()) {
            String subscriptionTopic = entry.getKey();
            // 使用 TopicMatcher 检查发布的主题是否匹配订阅的主题（订阅主题可能有通配符）
            if (TopicMatcher.matches(subscriptionTopic, topic)) {
                matchedClients.addAll(entry.getValue());
            }
        }

        if (matchedClients.isEmpty()) {
            log.log(Level.FINE, "No subscribers for topic: " + topic);
            return Mono.empty();
        }

        log.log(Level.FINE, "Publishing to topic " + topic + " for " + matchedClients.size() + " clients");

        return Flux.fromIterable(matchedClients)
                   .flatMap(clientId -> {
                       ServerConnection connection = connections.get(clientId);
                       if (connection == null) {
                           log.log(Level.WARNING, "Client " + clientId + " not found in connections");
                           return Mono.empty();
                       }

                       // 复制payload，因为每个客户端都需要独立的ByteBuf
                       ByteBuf clientPayload = payload.retainedDuplicate();

                       // 构建 MqttPublishMessage
                       int messageId = qos == io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE ? 0 : 1;
                       io.netty.handler.codec.mqtt.MqttPublishMessage publishMessage =
                               io.netty.handler.codec.mqtt.MqttMessageBuilders.publish()
                                                                              .topicName(topic)
                                                                              .payload(clientPayload)
                                                                              .qos(qos)
                                                                              .retained(retain)
                                                                              .messageId(messageId)
                                                                              .build();
                       return connection.publish(publishMessage)
                                        .doFinally(signal -> clientPayload.release())
                                        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic()); // 异步执行
                   })
                   .then();
    }

    /**
     * 发布MQTT消息到所有匹配的订阅者
     *
     * @param publisherClientId 发布者的客户端ID
     * @param message           MQTT发布消息
     * @return 发布完成的Mono
     */
    private Mono<Void> publish(String publisherClientId, MqttPublishMessage message) {
        String topic = message.variableHeader().topicName();
        ByteBuf payload = message.payload();
        MqttQoS qos = message.fixedHeader().qosLevel();
        boolean retain = message.fixedHeader().isRetain();

        return publish(publisherClientId, topic, payload, qos, retain);
    }

    /**
     * 获取当前连接的客户端数量
     *
     * @return 连接数
     */
    int getConnectionCount() {
        return connections.size();
    }

    /**
     * 获取当前订阅数量
     *
     * @return 订阅数
     */
    int getSubscriptionCount() {
        return subscriptions.values().stream()
                            .mapToInt(Set::size)
                            .sum();
    }

    /**
     * 获取指定客户端的所有订阅主题
     *
     * @param clientId 客户端ID
     * @return 订阅主题集合
     */
    Set<String> getClientSubscriptions(String clientId) {
        Set<String> topics = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : subscriptions.entrySet()) {
            if (entry.getValue().contains(clientId)) {
                topics.add(entry.getKey());
            }
        }
        return topics;
    }
}
