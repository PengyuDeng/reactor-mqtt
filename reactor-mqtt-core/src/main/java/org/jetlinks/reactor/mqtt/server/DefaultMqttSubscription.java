package org.jetlinks.reactor.mqtt.server;

import io.netty.handler.codec.mqtt.*;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * MQTT 订阅请求实现
 *
 * @author PengyuDeng
 */
public class DefaultMqttSubscription implements MqttSubscription {

    private static final VarHandle ACKNOWLEDGED;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            ACKNOWLEDGED = lookup.findVarHandle(DefaultMqttSubscription.class, "acknowledged", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final MqttSubscribeMessage message;
    private final DefaultServerConnection connection;
    @SuppressWarnings("unused")
    private volatile boolean acknowledged = false;

    public DefaultMqttSubscription(MqttSubscribeMessage message, DefaultServerConnection connection) {
        this.message = message;
        this.connection = connection;
    }

    @Override
    public MqttSubscribeMessage getMessage() {
        return message;
    }

    @Override
    public Mono<Void> acknowledge() {
        return Mono.defer(() -> {
            if (!ACKNOWLEDGED.compareAndSet(this, false, true)) {
                return Mono.empty();
            }

            MqttSubAckMessage subAck = MqttMessageBuilders.subAck()
                                                          .packetId(message.variableHeader().messageId())
                                                          .addGrantedQoses(message.payload().topicSubscriptions()
                                                                                  .stream()
                                                                                  .map(MqttTopicSubscription::qualityOfService)
                                                                                  .toArray(MqttQoS[]::new))
                                                          .build();
            return connection.send(subAck);
        });
    }
}
