package org.jetlinks.reactor.mqtt.broker;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.jetlinks.reactor.mqtt.MqttAuth;
import org.jetlinks.reactor.mqtt.MqttWillMessage;
import org.jetlinks.reactor.mqtt.Topic;
import org.jetlinks.reactor.mqtt.server.MqttSubscription;
import org.jetlinks.reactor.mqtt.server.MqttUnsubscription;
import org.jetlinks.reactor.mqtt.server.ServerConnection;
import org.jetlinks.reactor.mqtt.server.ServerReceivedPublish;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BrokerMessageRouterTest {

    @Test
    void shouldPropagateOldConnectionCloseErrorWhenReplacingConnection() {
        BrokerMessageRouter router = new BrokerMessageRouter();
        TestServerConnection oldConnection = new TestServerConnection();
        oldConnection.closeResult = Mono.error(new IllegalStateException("close failed"));

        StepVerifier.create(router.onConnectionAccepted("client-1", oldConnection))
                    .verifyComplete();

        StepVerifier.create(router.onConnectionAccepted("client-1", new TestServerConnection()))
                    .expectErrorMessage("close failed")
                    .verify();

        assertEquals(1, oldConnection.closeCalls.get());
        assertEquals(1, router.getConnectionCount());
    }

    @Test
    void shouldPublishToAllMatchedClients() {
        BrokerMessageRouter router = new BrokerMessageRouter();
        TestServerConnection exactConnection = new TestServerConnection();
        TestServerConnection wildcardConnection = new TestServerConnection();

        StepVerifier.create(router.onConnectionAccepted("client-1", exactConnection))
                    .verifyComplete();
        StepVerifier.create(router.onConnectionAccepted("client-2", wildcardConnection))
                    .verifyComplete();
        StepVerifier.create(router.onSubscribe("client-1", Topic.of("sensor/room1/temp")))
                    .verifyComplete();
        StepVerifier.create(router.onSubscribe("client-2", Topic.of("sensor/+/temp")))
                    .verifyComplete();

        MqttPublishMessage message = MqttMessageBuilders.publish()
                                                       .topicName("sensor/room1/temp")
                                                       .payload(Unpooled.wrappedBuffer(new byte[]{1}))
                                                       .qos(MqttQoS.AT_LEAST_ONCE)
                                                       .properties(MqttProperties.NO_PROPERTIES)
                                                       .build();
        TestServerReceivedPublish publish = new TestServerReceivedPublish(
                "publisher",
                Topic.of("sensor/room1/temp"),
                message
        );

        StepVerifier.create(router.onPublish("publisher", publish))
                    .verifyComplete();

        assertEquals(1, exactConnection.publishCalls.get());
        assertEquals(1, wildcardConnection.publishCalls.get());
        assertEquals(2, router.getSubscriptionCount());
        assertEquals("sensor/room1/temp", exactConnection.lastPublishedTopic.get());
        assertEquals("sensor/room1/temp", wildcardConnection.lastPublishedTopic.get());
    }

    @Test
    void shouldRouteUsingParsedTopicInsteadOfRawMessageTopicName() {
        BrokerMessageRouter router = new BrokerMessageRouter();
        TestServerConnection connection = new TestServerConnection();

        StepVerifier.create(router.onConnectionAccepted("client-1", connection))
                    .verifyComplete();
        StepVerifier.create(router.onSubscribe("client-1", Topic.of("sensor/room1/temp")))
                    .verifyComplete();

        MqttPublishMessage message = MqttMessageBuilders.publish()
                                                       .topicName("raw/other/topic")
                                                       .payload(Unpooled.wrappedBuffer(new byte[]{1}))
                                                       .qos(MqttQoS.AT_MOST_ONCE)
                                                       .properties(MqttProperties.NO_PROPERTIES)
                                                       .build();

        StepVerifier.create(router.onPublish(
                                    "publisher",
                                    new TestServerReceivedPublish("publisher", Topic.of("sensor/room1/temp"), message)
                            ))
                    .verifyComplete();

        assertEquals(1, connection.publishCalls.get());
        assertEquals("sensor/room1/temp", connection.lastPublishedTopic.get());
    }

    private static class TestServerConnection implements ServerConnection {

        private final AtomicInteger closeCalls = new AtomicInteger();
        private final AtomicInteger publishCalls = new AtomicInteger();
        private final AtomicReference<String> lastPublishedTopic = new AtomicReference<>();
        private Mono<Void> closeResult = Mono.empty();
        private Mono<Void> publishResult = Mono.empty();

        @Override
        public MqttAuth getAuth() {
            return MqttAuth.empty();
        }

        @Override
        public Mono<Void> reject(MqttConnectReturnCode code) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> accept() {
            return Mono.empty();
        }

        @Override
        public MqttWillMessage getWill() {
            return MqttWillMessage.EMPTY;
        }

        @Override
        public ServerConnection handlePublishing(Function<ServerReceivedPublish, Mono<Void>> handler) {
            return this;
        }

        @Override
        public ServerConnection handleSubscribe(Function<MqttSubscription, Mono<Void>> handler) {
            return this;
        }

        @Override
        public ServerConnection handleUnsubscribe(Function<MqttUnsubscription, Mono<Void>> handler) {
            return this;
        }

        @Override
        public ServerConnection autoAck(boolean autoAck) {
            return this;
        }

        @Override
        public long getLastPingTime() {
            return 0;
        }

        @Override
        public Duration getKeepAliveTimeout() {
            return Duration.ZERO;
        }

        @Override
        public Mono<Void> setKeepAliveTimeout(Duration duration) {
            return Mono.empty();
        }

        @Override
        public String getClientId() {
            return "test-client";
        }

        @Override
        public MqttVersion getVersion() {
            return MqttVersion.MQTT_3_1_1;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public boolean isAlive() {
            return true;
        }

        @Override
        public Mono<Void> onClose() {
            return Mono.empty();
        }

        @Override
        public Mono<Void> close() {
            return Mono.defer(() -> {
                closeCalls.incrementAndGet();
                return closeResult;
            });
        }

        @Override
        public Mono<Void> publish(MqttPublishMessage message) {
            return Mono.defer(() -> {
                publishCalls.incrementAndGet();
                lastPublishedTopic.set(message.variableHeader().topicName());
                return publishResult;
            });
        }
    }

    private static final class TestServerReceivedPublish implements ServerReceivedPublish {

        private final String clientId;
        private final Topic topic;
        private final MqttPublishMessage message;

        private TestServerReceivedPublish(String clientId, Topic topic, MqttPublishMessage message) {
            this.clientId = clientId;
            this.topic = topic;
            this.message = message;
        }

        @Override
        public String clientId() {
            return clientId;
        }

        @Override
        public Topic topic() {
            return topic;
        }

        @Override
        public MqttPublishMessage message() {
            return message;
        }

        @Override
        public MqttProperties properties() {
            return MqttProperties.NO_PROPERTIES;
        }

        @Override
        public Mono<Void> ack() {
            return Mono.empty();
        }

        @Override
        public Mono<Void> nack(MqttProperties properties) {
            return Mono.empty();
        }
    }
}
