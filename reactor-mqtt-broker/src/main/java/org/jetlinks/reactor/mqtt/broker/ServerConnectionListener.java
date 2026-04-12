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

import org.jetlinks.reactor.mqtt.Topic;
import org.jetlinks.reactor.mqtt.server.ServerConnection;
import org.jetlinks.reactor.mqtt.server.ServerReceivedPublish;
import reactor.core.publisher.Mono;

/**
 * MQTT Server 连接事件监听器接口
 *
 * <p>用于监听客户端的连接、订阅、取消订阅和发布等事件。
 * 通过实现此接口，可以扩展 Server 的功能，例如实现消息路由、持久化等。</p>
 *
 * @author PengyuDeng
 */
public interface ServerConnectionListener {

    /**
     * 当客户端连接被接受后触发
     *
     * @param clientId   客户端ID
     * @param connection 服务端连接对象
     * @return 处理完成的Mono
     */
    default Mono<Void> onConnectionAccepted(String clientId, ServerConnection connection) {
        return Mono.empty();
    }

    /**
     * 当客户端连接关闭时触发
     *
     * @param clientId 客户端ID
     * @return 处理完成的Mono
     */
    default Mono<Void> onConnectionClosed(String clientId) {
        return Mono.empty();
    }

    /**
     * 当客户端订阅主题时触发
     *
     * @param clientId 客户端ID
     * @param topic    订阅的主题
     * @return 处理完成的Mono
     */
    default Mono<Void> onSubscribe(String clientId, Topic topic) {
        return Mono.empty();
    }

    /**
     * 当客户端取消订阅主题时触发
     *
     * @param clientId 客户端ID
     * @param topic    取消订阅的主题
     * @return 处理完成的Mono
     */
    default Mono<Void> onUnsubscribe(String clientId, Topic topic) {
        return Mono.empty();
    }

    /**
     * 当客户端发布消息时触发
     *
     * @param clientId 客户端ID
     * @param publish  发布的消息
     * @return 处理完成的Mono
     */
    default Mono<Void> onPublish(String clientId, ServerReceivedPublish publish) {
        return Mono.empty();
    }

    /**
     * 创建一个空的监听器（不做任何处理）
     *
     * @return 空监听器
     */
    static ServerConnectionListener empty() {
        return new ServerConnectionListener() {
        };
    }
}
