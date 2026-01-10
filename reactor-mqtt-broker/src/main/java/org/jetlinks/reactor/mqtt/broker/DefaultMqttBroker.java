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
import io.netty.handler.ssl.SslContext;
import org.jetlinks.reactor.mqtt.server.DefaultMqttServer;
import org.jetlinks.reactor.mqtt.server.ServerConnection;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.netty.resources.LoopResources;

import java.time.Duration;
import java.util.function.Function;

/**
 * MQTT Broker 默认实现
 *
 * <p>通过继承 {@link DefaultMqttServer} 并覆写连接处理逻辑，集成 {@link BrokerMessageRouter} 实现消息路由功能。</p>
 *
 * @author PengyuDeng
 */
class DefaultMqttBroker extends DefaultMqttServer implements MqttBroker {

    private final BrokerMessageRouter router;

    DefaultMqttBroker() {
        super();
        this.router = new BrokerMessageRouter();
    }

    @Override
    public MqttBroker host(String host) {
        super.host(host);
        return this;
    }

    @Override
    public MqttBroker port(int port) {
        super.port(port);
        return this;
    }

    @Override
    public MqttBroker maxMessageSize(int maxMessageSize) {
        super.maxMessageSize(maxMessageSize);
        return this;
    }

    @Override
    public MqttBroker idleTimeout(Duration idleTimeout) {
        super.idleTimeout(idleTimeout);
        return this;
    }

    @Override
    public MqttBroker ssl(SslContext sslContext) {
        super.ssl(sslContext);
        return this;
    }

    @Override
    public MqttBroker loopResources(LoopResources loopResources) {
        super.loopResources(loopResources);
        return this;
    }

    @Override
    public MqttBroker workerCount(int workerCount) {
        super.workerCount(workerCount);
        return this;
    }

    @Override
    public MqttBroker tcpNoDelay(boolean tcpNoDelay) {
        super.tcpNoDelay(tcpNoDelay);
        return this;
    }

    @Override
    public MqttBroker tcpKeepAlive(boolean tcpKeepAlive) {
        super.tcpKeepAlive(tcpKeepAlive);
        return this;
    }

    @Override
    public MqttBroker soBacklog(int soBacklog) {
        super.soBacklog(soBacklog);
        return this;
    }

    @Override
    public MqttBroker writeBufferWaterMark(int low, int high) {
        super.writeBufferWaterMark(low, high);
        return this;
    }

    @Override
    protected Publisher<Void> handle(NettyInbound inbound, NettyOutbound outbound) {
        // 覆写父类的 handle 方法，使用 BrokerServerConnection 注入消息路由功能
        BrokerServerConnection brokerConnection = new BrokerServerConnection(inbound, outbound, router);
        return brokerConnection.run(conn -> super.invokeHandler(conn));
    }

    @Override
    public int getConnectionCount() {
        return router.getConnectionCount();
    }

    @Override
    public int getSubscriptionCount() {
        return router.getSubscriptionCount();
    }
}
