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

import io.netty.handler.ssl.SslContext;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

import java.time.Duration;
import java.util.function.Function;

/**
 * MQTT 服务端配置类
 *
 * <p>封装 MQTT Server 的所有配置参数，支持通过 fluent API 或直接设置属性进行配置。</p>
 *
 * @author PengyuDeng
 */
public class MqttServerConfig {

    /**
     * 服务器绑定的主机地址
     */
    private String host = "0.0.0.0";

    /**
     * 服务器绑定的端口
     */
    private int port = 1883;

    /**
     * 最大消息大小（字节）
     */
    private int maxMessageSize = 8096;

    /**
     * 空闲超时时间，超过此时间没有任何读写操作则关闭连接
     */
    private Duration idleTimeout;

    /**
     * SSL/TLS 上下文
     */
    private SslContext sslContext;

    /**
     * Netty 事件循环资源
     */
    private LoopResources loopResources;

    /**
     * 工作线程数
     */
    private int workerCount = Runtime.getRuntime().availableProcessors();

    /**
     * 是否启用 TCP_NODELAY（禁用 Nagle 算法）
     */
    private boolean tcpNoDelay = true;

    /**
     * 是否启用 TCP Keep-Alive
     */
    private boolean tcpKeepAlive = true;

    /**
     * TCP 连接队列大小
     */
    private int soBacklog = 1024;

    /**
     * 写缓冲区低水位
     */
    private int writeBufferLow = 32 * 1024;

    /**
     * 写缓冲区高水位
     */
    private int writeBufferHigh = 64 * 1024;

    /**
     * 连接处理器
     */
    private Function<ServerConnection, Mono<Void>> connectionHandler;

    /**
     * 认证器
     */
    private MqttAuthenticator authenticator = MqttAuthenticator.allowAnonymous();

    /**
     * 是否自动确认消息
     */
    private boolean autoAck = true;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be null or blank");
        }
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        this.port = port;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        if (maxMessageSize <= 0) {
            throw new IllegalArgumentException("maxMessageSize must be positive");
        }
        this.maxMessageSize = maxMessageSize;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    public LoopResources getLoopResources() {
        return loopResources;
    }

    public void setLoopResources(LoopResources loopResources) {
        this.loopResources = loopResources;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive");
        }
        this.workerCount = workerCount;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public boolean isTcpKeepAlive() {
        return tcpKeepAlive;
    }

    public void setTcpKeepAlive(boolean tcpKeepAlive) {
        this.tcpKeepAlive = tcpKeepAlive;
    }

    public int getSoBacklog() {
        return soBacklog;
    }

    public void setSoBacklog(int soBacklog) {
        if (soBacklog <= 0) {
            throw new IllegalArgumentException("soBacklog must be positive");
        }
        this.soBacklog = soBacklog;
    }

    public int getWriteBufferLow() {
        return writeBufferLow;
    }

    public int getWriteBufferHigh() {
        return writeBufferHigh;
    }

    public void setWriteBufferWaterMark(int low, int high) {
        if (low < 0 || high < 0) {
            throw new IllegalArgumentException("writeBufferWaterMark values must not be negative");
        }
        if (low > high) {
            throw new IllegalArgumentException("writeBufferLow must not be greater than writeBufferHigh");
        }
        this.writeBufferLow = low;
        this.writeBufferHigh = high;
    }

    public Function<ServerConnection, Mono<Void>> getConnectionHandler() {
        return connectionHandler;
    }

    public void setConnectionHandler(Function<ServerConnection, Mono<Void>> connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    public MqttAuthenticator getAuthenticator() {
        return authenticator;
    }

    public void setAuthenticator(MqttAuthenticator authenticator) {
        this.authenticator = authenticator != null ? authenticator : MqttAuthenticator.allowAnonymous();
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }
}
