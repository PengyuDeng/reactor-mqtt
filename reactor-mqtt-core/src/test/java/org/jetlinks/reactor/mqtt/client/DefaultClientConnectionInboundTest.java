package org.jetlinks.reactor.mqtt.client;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;
import reactor.netty.tcp.TcpClient;
import reactor.test.StepVerifier;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultClientConnectionInboundTest {

    @Test
    void shouldProcessAckWhilePublishHandlerWaitsForPublishAck() throws Exception {
        AtomicInteger outboundPublishCount = new AtomicInteger();
        MqttClientConfig config = new MqttClientConfig();
        config.setPublishTimeout(Duration.ofSeconds(1));

        DefaultClientConnection connection = new DefaultClientConnection(
                newFakeNettyConnection(outboundPublishCount),
                config,
                TcpClient::create
        );

        config.setPublishingHandler(ignored -> connection.publish(
                "outbound/topic",
                Unpooled.wrappedBuffer(new byte[]{1}),
                MqttQoS.AT_LEAST_ONCE,
                false
        ));

        Method setConnectedFlag = DefaultClientConnection.class.getDeclaredMethod("setConnectedFlag");
        setConnectedFlag.setAccessible(true);
        setConnectedFlag.invoke(connection);

        MqttPublishMessage inboundPublish = MqttMessageBuilders.publish()
                                                               .topicName("inbound/topic")
                                                               .payload(Unpooled.wrappedBuffer(new byte[]{1}))
                                                               .qos(MqttQoS.AT_MOST_ONCE)
                                                               .build();
        MqttMessage pubAck = new MqttPubAckMessage(
                new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(1)
        );

        try {
            StepVerifier.create(connection.handleInboundMessages(Flux.just(inboundPublish, pubAck)))
                        .expectComplete()
                        .verify(Duration.ofSeconds(2));
        } finally {
            ReferenceCountUtil.safeRelease(inboundPublish);
        }

        assertEquals(1, outboundPublishCount.get());
    }

    private Connection newFakeNettyConnection(AtomicInteger outboundPublishCount) {
        NettyOutbound outbound = (NettyOutbound) Proxy.newProxyInstance(
                NettyOutbound.class.getClassLoader(),
                new Class[]{NettyOutbound.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "sendObject" -> {
                        Object message = Mono.from((Publisher<?>) args[0]).block(Duration.ofSeconds(1));
                        outboundPublishCount.incrementAndGet();
                        ReferenceCountUtil.safeRelease(message);
                        yield proxy;
                    }
                    case "then" -> Mono.empty();
                    default -> defaultValue(method.getReturnType());
                }
        );

        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "outbound" -> outbound;
            case "dispose" -> null;
            case "isDisposed" -> false;
            default -> defaultValue(method.getReturnType());
        };

        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class[]{Connection.class},
                handler
        );
    }

    private Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }
}
