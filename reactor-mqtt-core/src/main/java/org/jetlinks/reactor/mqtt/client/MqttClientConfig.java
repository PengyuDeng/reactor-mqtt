package org.jetlinks.reactor.mqtt.client;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.ssl.SslContext;
import org.jetlinks.reactor.mqtt.MqttConstants;
import org.jetlinks.reactor.mqtt.MqttWillMessage;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;

/**
 * MQTT 客户端配置类
 *
 * <p><b>重要约定:</b></p>
 * <ul>
 *   <li>在调用 {@code MqttClient.connect()} 或 {@code connectNow()} 建立连接之前,可以自由修改配置</li>
 *   <li>连接建立后,<b>不应该</b>修改配置参数,除非希望在下次重连时生效</li>
 *   <li>以下配置在连接建立时已发送给服务器,修改后不会影响当前连接,但会在重连时生效:
 *     <ul>
 *       <li>{@code clientId} - 客户端标识</li>
 *       <li>{@code username/password} - 认证信息</li>
 *       <li>{@code keepAlive} - 心跳间隔</li>
 *       <li>{@code cleanSession} - 会话清理标志</li>
 *       <li>{@code protocolVersion} - MQTT 协议版本</li>
 *       <li>{@code willMessage} - 遗嘱消息</li>
 *     </ul>
 *   </li>
 *   <li>以下配置可以在连接运行时修改,重连时会使用新值:
 *     <ul>
 *       <li>{@code reconnectStrategy} - 重连策略</li>
 *       <li>{@code autoResubscribe} - 自动重新订阅</li>
 *       <li>{@code publishTimeout/subscribeTimeout/unsubscribeTimeout} - 超时配置</li>
 *     </ul>
 *   </li>
 *   <li>为了线程安全,建议在连接建立后不修改任何配置</li>
 * </ul>
 *
 * @author PengyuDeng
 * @version reactor-mqtt 1.0
 * @date 2026/1/17 12:29
 */
public class MqttClientConfig {

    private String host = "127.0.0.1";
    private int port = 1883;
    private String clientId;
    private String username;
    private byte[] password;
    private int keepAlive = 60;
    private boolean cleanSession = true;
    private byte protocolVersion = MqttVersion.MQTT_3_1_1.protocolLevel();
    private int maxMessageSize = 8096;
    private MqttWillMessage willMessage;
    private SslContext sslContext;
    private ReconnectStrategy reconnectStrategy = ReconnectStrategy.none();
    private boolean autoResubscribe = true;
    private Function<ClientReceivedPublish, Mono<Void>> publishingHandler;
    private boolean autoAck = true;
    private MqttQoS qos = MqttQoS.AT_MOST_ONCE;
    private LoopResources loopResources;
    private boolean tcpNoDelay = true;
    private Duration connectTimeout = MqttConstants.Time.TEN_SECONDS;
    private Duration subscribeTimeout = MqttConstants.Time.TEN_SECONDS;
    private Duration unsubscribeTimeout = MqttConstants.Time.TEN_SECONDS;
    private Duration publishTimeout = MqttConstants.Time.THIRTY_SECONDS;
    private SubscriptionManager subscriptionManager;

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

    public String getClientId() {
        if (clientId == null || clientId.isBlank()) {
            setClientId("reactor-mqtt-" + UUID.randomUUID().toString().substring(0, 8));
        }
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public int getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(int keepAlive) {
        if (keepAlive < 0) {
            throw new IllegalArgumentException("keepAlive must not be negative");
        }
        this.keepAlive = keepAlive;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public byte getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(byte protocolVersion) {
        this.protocolVersion = protocolVersion;
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

    public MqttWillMessage getWillMessage() {
        return willMessage;
    }

    public void setWillMessage(MqttWillMessage willMessage) {
        this.willMessage = willMessage;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    public ReconnectStrategy getReconnectStrategy() {
        return reconnectStrategy;
    }

    public void setReconnectStrategy(ReconnectStrategy reconnectStrategy) {
        this.reconnectStrategy = reconnectStrategy;
    }

    public boolean isAutoResubscribe() {
        return autoResubscribe;
    }

    public void setAutoResubscribe(boolean autoResubscribe) {
        this.autoResubscribe = autoResubscribe;
    }

    public Function<ClientReceivedPublish, Mono<Void>> getPublishingHandler() {
        return publishingHandler;
    }

    public void setPublishingHandler(Function<ClientReceivedPublish, Mono<Void>> publishingHandler) {
        this.publishingHandler = publishingHandler;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

    public MqttQoS getQos() {
        return qos;
    }

    public void setQos(MqttQoS qos) {
        if (qos == null) {
            throw new IllegalArgumentException("defaultQos must not be null");
        }
        this.qos = qos;
    }

    public LoopResources getLoopResources() {
        return loopResources;
    }

    public void setLoopResources(LoopResources loopResources) {
        this.loopResources = loopResources;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Duration getSubscribeTimeout() {
        return subscribeTimeout;
    }

    public void setSubscribeTimeout(Duration subscribeTimeout) {
        this.subscribeTimeout = subscribeTimeout;
    }

    public Duration getUnsubscribeTimeout() {
        return unsubscribeTimeout;
    }

    public void setUnsubscribeTimeout(Duration unsubscribeTimeout) {
        this.unsubscribeTimeout = unsubscribeTimeout;
    }

    public Duration getPublishTimeout() {
        return publishTimeout;
    }

    public void setPublishTimeout(Duration publishTimeout) {
        this.publishTimeout = publishTimeout;
    }

    public SubscriptionManager getSubscriptionManager() {
        return subscriptionManager;
    }

    public void setSubscriptionManager(SubscriptionManager subscriptionManager) {
        this.subscriptionManager = subscriptionManager;
    }
}
