package org.jetlinks.reactor.mqtt.client;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.jetlinks.reactor.mqtt.Topic;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class SubscriptionManagerUnitTest {

    @Test
    void factorySubscriptionManagerShouldCreateTrieBasedImplementation() {
        SubscriptionManager manager = SubscriptionManager.create();
        assertSame(TrieBasedSubscriptionManager.class, manager.getClass());
    }

    @Test
    void trieSubscriptionManagerShouldNotRemoveReplacementHandlers() throws Exception {
        TrieBasedSubscriptionManager manager = new TrieBasedSubscriptionManager();
        TestClientConnection connection = new TestClientConnection();

        Disposable oldSubscription = manager.subscribe(connection, "test/topic", MqttQoS.AT_MOST_ONCE, msg -> Mono.empty());

        Map<String, Object> subscriptions = getField(manager, "subscriptionsMap");
        Object replacementHandlers = newTrieHandlers(connection, "test/topic", MqttQoS.AT_MOST_ONCE);
        subscriptions.put("test/topic", replacementHandlers);

        oldSubscription.dispose();

        assertSame(replacementHandlers, subscriptions.get("test/topic"));
    }

    @Test
    void factorySubscriptionManagerShouldDispatchAllMatchingHandlersAndIgnoreHandlerErrors() {
        SubscriptionManager manager = SubscriptionManager.create();
        AtomicInteger invoked = new AtomicInteger();

        manager.subscribe(new TestClientConnection(), "test/+", MqttQoS.AT_MOST_ONCE, msg -> {
            invoked.incrementAndGet();
            return Mono.empty();
        });
        manager.subscribe(new TestClientConnection(), "test/topic", MqttQoS.AT_MOST_ONCE, msg -> {
            invoked.incrementAndGet();
            throw new IllegalStateException("boom");
        });
        manager.subscribe(new TestClientConnection(), "other/topic", MqttQoS.AT_MOST_ONCE, msg -> {
            invoked.incrementAndGet();
            return Mono.empty();
        });

        StepVerifier.create(manager.handleMessage(newPublishing("test/topic")))
                    .verifyComplete();

        assertEquals(2, invoked.get());
    }

    @Test
    void trieSubscriptionManagerShouldDispatchAllMatchingHandlersAndIgnoreHandlerErrors() {
        TrieBasedSubscriptionManager manager = new TrieBasedSubscriptionManager();
        AtomicInteger invoked = new AtomicInteger();

        manager.subscribe(new TestClientConnection(), "test/+", MqttQoS.AT_MOST_ONCE, msg -> {
            invoked.incrementAndGet();
            return Mono.empty();
        });
        manager.subscribe(new TestClientConnection(), "test/topic", MqttQoS.AT_MOST_ONCE, msg -> {
            invoked.incrementAndGet();
            throw new IllegalStateException("boom");
        });
        manager.subscribe(new TestClientConnection(), "other/topic", MqttQoS.AT_MOST_ONCE, msg -> {
            invoked.incrementAndGet();
            return Mono.empty();
        });

        StepVerifier.create(manager.handleMessage(newPublishing("test/topic")))
                    .verifyComplete();

        assertEquals(2, invoked.get());
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(Object target, String name) throws Exception {
        Class<?> type = target.getClass();
        while (type != null) {
            try {
                Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                return (T) field.get(target);
            } catch (NoSuchFieldException ignore) {
                type = type.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    private Object newTrieHandlers(ClientConnection connection,
                                   String topic,
                                   MqttQoS qos) throws Exception {
        Class<?> type = Class.forName("org.jetlinks.reactor.mqtt.client.TrieBasedSubscriptionManager$TrieBasedSubscriptionHandlers");
        Constructor<?> constructor = type.getDeclaredConstructor(String.class, MqttQoS.class, ClientConnection.class);
        constructor.setAccessible(true);
        return constructor.newInstance(topic, qos, connection);
    }

    private ClientReceivedPublish newPublishing(String topic) {
        return new TestClientReceivedPublish(Topic.of(topic));
    }

    private static class TestClientConnection implements ClientConnection {

        @Override
        public MqttQoS getQos() {
            return MqttQoS.AT_MOST_ONCE;
        }

        @Override
        public Mono<Void> publish(String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
            return Mono.empty();
        }

        @Override
        public Disposable subscribe(Collection<String> topic, MqttQoS qos, Function<ClientReceivedPublish, Mono<Void>> handler) {
            return () -> {
            };
        }

        @Override
        public Mono<Void> unsubscribe(String... topics) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> unsubscribe(Collection<String> topics) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> disconnect() {
            return Mono.empty();
        }

        @Override
        public Flux<Integer> onReconnect() {
            return Flux.empty();
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
            return false;
        }

        @Override
        public Mono<Void> onClose() {
            return Mono.empty();
        }

        @Override
        public Mono<Void> close() {
            return Mono.empty();
        }

        @Override
        public Mono<Void> publish(MqttPublishMessage message) {
            return Mono.empty();
        }
    }

    private static class TestClientReceivedPublish implements ClientReceivedPublish {

        private final Topic topic;

        private TestClientReceivedPublish(Topic topic) {
            this.topic = topic;
        }

        @Override
        public Topic topic() {
            return topic;
        }

        @Override
        public MqttPublishMessage message() {
            return null;
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
