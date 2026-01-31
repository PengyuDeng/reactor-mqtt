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
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.jetlinks.reactor.mqtt.Topic;
import org.jetlinks.reactor.mqtt.TopicTrie;
import org.jetlinks.reactor.mqtt.server.ServerConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
     * 基于通用 TopicTrie 的订阅索引，提供 O(L) 复杂度的主题匹配
     * L = 主题层级数（通常 3-5），远优于线性遍历的 O(N)
     */
    private final TopicTrie<String> subscriptionTrie = new TopicTrie<>();

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
        // 使用通用 TopicTrie 批量清理该客户端的所有订阅
        subscriptionTrie.removeAll(clientId);
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
            log.log(Level.INFO, () -> "Client " + clientId + " reconnected, closing old connection");
            old.close().subscribe();
        }

        log.log(Level.FINE, () -> "Registered connection for client: " + clientId);
    }

    /**
     * 取消注册客户端连接，并清理所有订阅
     *
     * @param clientId 客户端ID
     */
    private void unregisterConnection(String clientId) {
        connections.remove(clientId);
        // Trie 树会在 onConnectionClosed 中统一清理
        log.log(Level.FINE, () -> "Unregistered connection for client: " + clientId);
    }

    /**
     * 添加订阅到 Trie 树
     *
     * @param clientId 客户端ID
     * @param topic    订阅的主题（可能包含通配符）
     */
    private void addSubscription(String clientId, String topic) {
        subscriptionTrie.addSubscription(Topic.of(topic).getLevels(), clientId);
        log.log(Level.FINE, () -> "Client " + clientId + " subscribed to: " + topic);
    }

    /**
     * 从 Trie 树移除订阅
     *
     * @param clientId 客户端ID
     * @param topic    要取消订阅的主题
     */
    private void removeSubscription(String clientId, String topic) {
        subscriptionTrie.removeSubscription(Topic.of(topic).getLevels(), clientId);
        log.log(Level.FINE, () -> "Client " + clientId + " unsubscribed from: " + topic);
    }

    /**
     * 发布消息到所有匹配的订阅者
     *
     * <p>使用 Trie 树快速查找匹配的订阅者，时间复杂度 O(L)，L = 主题层级数</p>
     *
     * @param publisherClientId 发布者的客户端ID（可能为null）
     * @param topic             消息主题（具体的主题路径，不含通配符）
     * @param payload           消息负载
     * @param qos               QoS级别
     * @param retain            是否保留
     * @return 发布完成的Mono
     */
    private Mono<Void> publish(String publisherClientId, String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        // 使用 Trie 树快速查找所有匹配的订阅者 - O(L) 复杂度
        Set<String> matchedClients = subscriptionTrie.findMatches(Topic.of(topic).getLevels());

        if (matchedClients.isEmpty()) {
            log.log(Level.FINE, () -> "No subscribers for topic: " + topic);
            return Mono.empty();
        }

        log.log(Level.FINE, () -> "Publishing to topic " + topic + " for " + matchedClients.size() + " clients");

        return Flux.fromIterable(matchedClients)
                   .flatMap(clientId -> {
                       ServerConnection connection = connections.get(clientId);
                       if (connection == null) {
                           log.log(Level.WARNING, () -> "Client " + clientId + " not found in connections");
                           return Mono.empty();
                       }

                       return Mono.using(payload::retainedDuplicate,
                                         clientPayload -> {
                                             int messageId = qos == MqttQoS.AT_MOST_ONCE ? 0 : 1;
                                             MqttPublishMessage publishMessage = MqttMessageBuilders.publish()
                                                                                                    .topicName(topic)
                                                                                                    .payload(clientPayload)
                                                                                                    .qos(qos)
                                                                                                    .retained(retain)
                                                                                                    .messageId(messageId)
                                                                                                    .build();
                                             return connection.publish(publishMessage);
                                         },
                                         ByteBuf::release
                       ).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
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
     * @return 订阅数（使用 Trie 树统计）
     */
    int getSubscriptionCount() {
        return subscriptionTrie.getTotalSubscriptionCount();
    }
}
