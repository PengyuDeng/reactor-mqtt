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

import io.netty.handler.ssl.SslContext;
import org.jetlinks.reactor.mqtt.server.DefaultMqttServer;
import org.jetlinks.reactor.mqtt.server.MqttServer;
import org.jetlinks.reactor.mqtt.server.ServerConnection;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;

import java.time.Duration;
import java.util.function.Function;

/**
 * MQTT Broker 默认实现
 *
 * <p>基于 {@link DefaultMqttServer} 和 {@link BrokerMessageRouter} 实现完整的 MQTT Broker 功能。</p>
 *
 * @author PengyuDeng
 */
class DefaultMqttBroker implements MqttBroker {

    private final DefaultMqttServer server;
    private final BrokerMessageRouter router;

    DefaultMqttBroker() {
        this.server = new DefaultMqttServer();
        this.router = new BrokerMessageRouter();
        // 将路由器设置为服务器的连接监听器
        this.server.connectionListener(router);
    }

    @Override
    public MqttBroker host(String host) {
        server.host(host);
        return this;
    }

    @Override
    public MqttBroker port(int port) {
        server.port(port);
        return this;
    }

    @Override
    public MqttBroker maxMessageSize(int maxMessageSize) {
        server.maxMessageSize(maxMessageSize);
        return this;
    }

    @Override
    public MqttBroker idleTimeout(Duration idleTimeout) {
        server.idleTimeout(idleTimeout);
        return this;
    }

    @Override
    public MqttBroker ssl(SslContext sslContext) {
        server.ssl(sslContext);
        return this;
    }

    @Override
    public MqttBroker loopResources(LoopResources loopResources) {
        server.loopResources(loopResources);
        return this;
    }

    @Override
    public MqttBroker workerCount(int workerCount) {
        server.workerCount(workerCount);
        return this;
    }

    @Override
    public MqttBroker tcpNoDelay(boolean tcpNoDelay) {
        server.tcpNoDelay(tcpNoDelay);
        return this;
    }

    @Override
    public MqttBroker tcpKeepAlive(boolean tcpKeepAlive) {
        server.tcpKeepAlive(tcpKeepAlive);
        return this;
    }

    @Override
    public MqttBroker soBacklog(int soBacklog) {
        server.soBacklog(soBacklog);
        return this;
    }

    @Override
    public MqttBroker writeBufferWaterMark(int low, int high) {
        server.writeBufferWaterMark(low, high);
        return this;
    }

    @Override
    public MqttServer handle(Function<ServerConnection, Mono<Void>> handler) {
        server.handle(handler);
        return this;
    }

    @Override
    public MqttServer authenticator(org.jetlinks.reactor.mqtt.server.MqttAuthenticator authenticator) {
        server.authenticator(authenticator);
        return this;
    }

    @Override
    public DisposableServer bindNow() {
        return server.bindNow();
    }

    @Override
    public DisposableServer bindNow(Duration timeout) {
        return server.bindNow(timeout);
    }

    @Override
    public Mono<? extends DisposableServer> bind() {
        return server.bind();
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
