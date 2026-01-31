/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
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
package org.jetlinks.reactor.mqtt.server;

import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 Reactor Netty 的 MQTT Server - 纯响应式、高性能
 *
 * @author PengyuDeng
 */
public class DefaultMqttServer implements MqttServer {

    private static final Logger log = Logger.getLogger(DefaultMqttServer.class.getName());

    private final MqttServerConfig config;

    /**
     * 创建一个新的 DefaultMqttServer 实例
     */
    public DefaultMqttServer() {
        this.config = new MqttServerConfig();
    }

    /**
     * 创建一个使用指定配置的 DefaultMqttServer 实例
     *
     * @param config 服务端配置
     */
    public DefaultMqttServer(MqttServerConfig config) {
        this.config = config != null ? config : new MqttServerConfig();
    }

    /**
     * 获取服务端配置
     *
     * @return 服务端配置
     */
    public MqttServerConfig getConfig() {
        return config;
    }

    @Override
    public MqttServer host(String host) {
        config.setHost(host);
        return this;
    }

    @Override
    public MqttServer port(int port) {
        config.setPort(port);
        return this;
    }

    @Override
    public MqttServer maxMessageSize(int maxMessageSize) {
        config.setMaxMessageSize(maxMessageSize);
        return this;
    }

    @Override
    public MqttServer idleTimeout(Duration idleTimeout) {
        config.setIdleTimeout(idleTimeout);
        return this;
    }

    @Override
    public MqttServer ssl(SslContext sslContext) {
        config.setSslContext(sslContext);
        return this;
    }

    @Override
    public MqttServer loopResources(LoopResources loopResources) {
        config.setLoopResources(loopResources);
        return this;
    }

    @Override
    public MqttServer workerCount(int workerCount) {
        config.setWorkerCount(workerCount);
        return this;
    }

    @Override
    public MqttServer tcpNoDelay(boolean tcpNoDelay) {
        config.setTcpNoDelay(tcpNoDelay);
        return this;
    }

    @Override
    public MqttServer tcpKeepAlive(boolean tcpKeepAlive) {
        config.setTcpKeepAlive(tcpKeepAlive);
        return this;
    }

    @Override
    public MqttServer soBacklog(int soBacklog) {
        config.setSoBacklog(soBacklog);
        return this;
    }

    @Override
    public MqttServer writeBufferWaterMark(int low, int high) {
        config.setWriteBufferWaterMark(low, high);
        return this;
    }

    @Override
    public MqttServer handle(Function<ServerConnection, Mono<Void>> handler) {
        config.setConnectionHandler(handler);
        return this;
    }

    @Override
    public MqttServer authenticator(MqttAuthenticator authenticator) {
        config.setAuthenticator(authenticator);
        return this;
    }

    @Override
    public MqttServer autoAck(boolean autoAck) {
        config.setAutoAck(autoAck);
        return this;
    }

    @Override
    public Mono<? extends DisposableServer> bind() {
        return createTcpServer().bind();
    }

    @Override
    public DisposableServer bindNow() {
        return createTcpServer().bindNow();
    }

    @Override
    public DisposableServer bindNow(Duration timeout) {
        return createTcpServer().bindNow(timeout);
    }

    private TcpServer createTcpServer() {
        TcpServer server = TcpServer.create()
                                    .host(config.getHost())
                                    .port(config.getPort())
                                    .runOn(getLoopResources())
                                    .option(ChannelOption.SO_REUSEADDR, true)
                                    .option(ChannelOption.SO_BACKLOG, config.getSoBacklog())
                                    .childOption(ChannelOption.TCP_NODELAY, config.isTcpNoDelay())
                                    .childOption(ChannelOption.SO_KEEPALIVE, config.isTcpKeepAlive())
                                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                                                 new WriteBufferWaterMark(config.getWriteBufferLow(), config.getWriteBufferHigh()))
                                    .doOnConnection(this::initPipeline)
                                    .handle(this::handle);

        return config.getSslContext() != null
                ? server.secure(spec -> spec.sslContext(config.getSslContext()))
                : server;
    }

    private LoopResources getLoopResources() {
        return config.getLoopResources() != null
                ? config.getLoopResources()
                : LoopResources.create("mqtt-", config.getWorkerCount(), true);
    }

    private void initPipeline(Connection connection) {
        connection.addHandlerFirst("mqttEncoder", MqttEncoder.INSTANCE);
        connection.addHandlerFirst("mqttDecoder", new MqttDecoder(config.getMaxMessageSize()));

        Duration idleTimeout = config.getIdleTimeout();
        if (idleTimeout != null && !idleTimeout.isZero()) {
            connection.addHandlerFirst("idleStateHandler", new IdleStateHandler(0, 0, idleTimeout.toSeconds(), TimeUnit.SECONDS));
        }
    }

    protected Publisher<Void> handle(NettyInbound inbound, NettyOutbound outbound) {
        return new DefaultServerConnection(inbound, outbound, config.isAutoAck()).run(this::invokeHandler);
    }

    protected Mono<Void> invokeHandler(ServerConnection serverConnection) {
        MqttAuthenticator authenticator = config.getAuthenticator();
        Function<ServerConnection, Mono<Void>> connectionHandler = config.getConnectionHandler();

        // 先进行认证
        return authenticator.authenticate(serverConnection)
                            .flatMap(returnCode -> {
                                if (returnCode != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
                                    log.log(Level.WARNING, () -> "Client " + serverConnection.getClientId() + " authentication failed: " + returnCode);
                                    return serverConnection.reject(returnCode);
                                }

                                // 认证通过，执行用户的 handler
                                if (connectionHandler == null) {
                                    return serverConnection.accept();
                                }
                                return connectionHandler.apply(serverConnection)
                                                        .onErrorResume(err -> {
                                                            log.log(Level.SEVERE, err, () -> "Error handling MQTT connection for client " + serverConnection.getClientId() + ": " + err.getMessage());
                                                            return serverConnection.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                                                        });
                            });
    }
}
