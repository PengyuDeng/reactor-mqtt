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

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.jetlinks.reactor.mqtt.server.DefaultServerConnection;
import reactor.core.publisher.Mono;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;

/**
 * Broker 专用的 ServerConnection 实现，集成消息路由功能
 *
 * <p>继承 DefaultServerConnection，在其基础上添加与 BrokerMessageRouter 的集成。
 * 当客户端进行 PUBLISH、SUBSCRIBE、UNSUBSCRIBE 操作时，通知 router 进行消息路由。</p>
 *
 * @author PengyuDeng
 */
class BrokerServerConnection extends DefaultServerConnection {

    private final ServerConnectionListener listener;

    /**
     * 创建一个 Broker 专用连接
     *
     * @param inbound  入站处理器
     * @param outbound 出站处理器
     * @param listener 连接事件监听器（通常是 BrokerMessageRouter）
     */
    BrokerServerConnection(NettyInbound inbound, NettyOutbound outbound, ServerConnectionListener listener) {
        super(inbound, outbound);
        this.listener = listener;

        // 设置消息处理器，拦截 PUBLISH、SUBSCRIBE、UNSUBSCRIBE 事件并通知 listener
        super.handlePublishing(publish -> {
            String clientId = getClientId();
            // 使用 getOrigin() 获取原始 MqttPublishMessage
            MqttPublishMessage message = publish.getOrigin();

            // 通知 listener，由 router 处理消息路由
            listener.onPublish(clientId, message).subscribe();
        });

        super.onSubscribe(subscription -> {
            String clientId = getClientId();

            // 从 MqttSubscribeMessage 中提取所有订阅主题
            subscription.getMessage().payload().topicSubscriptions().forEach(topicSub -> {
                String topic = topicSub.topicFilter();
                listener.onSubscribe(clientId, topic).subscribe();
            });
        });

        super.onUnsubscribe(unsubscription -> {
            String clientId = getClientId();

            // 从 MqttUnsubscribeMessage 中提取所有取消订阅的主题
            unsubscription.getMessage().payload().topics().forEach(topic -> {
                listener.onUnsubscribe(clientId, topic).subscribe();
            });
        });
    }

    @Override
    public Mono<Void> accept() {
        return super.accept()
                    .then(Mono.defer(() -> {
                        // 连接被接受后，通知 listener
                        String clientId = getClientId();
                        return listener.onConnectionAccepted(clientId, this);
                    }));
    }

    @Override
    public Mono<Void> close() {
        String clientId = getClientId();
        return listener.onConnectionClosed(clientId)
                       .then(super.close());
    }
}
