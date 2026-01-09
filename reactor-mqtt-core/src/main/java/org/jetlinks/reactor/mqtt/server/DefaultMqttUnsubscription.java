package org.jetlinks.reactor.mqtt.server;

import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * MQTT 取消订阅请求实现
 *
 * @author PengyuDeng
 */
public class DefaultMqttUnsubscription implements MqttUnsubscription {

    private static final VarHandle ACKNOWLEDGED;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            ACKNOWLEDGED = lookup.findVarHandle(DefaultMqttUnsubscription.class, "acknowledged", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final MqttUnsubscribeMessage message;
    private final DefaultServerConnection connection;
    @SuppressWarnings("unused") // accessed via VarHandle
    private volatile boolean acknowledged = false;

    public DefaultMqttUnsubscription(MqttUnsubscribeMessage message, DefaultServerConnection connection) {
        this.message = message;
        this.connection = connection;
    }

    @Override
    public MqttUnsubscribeMessage getMessage() {
        return message;
    }

    @Override
    public Mono<Void> acknowledge() {
        return Mono.defer(() -> {
            if (!ACKNOWLEDGED.compareAndSet(this, false, true)) {
                return Mono.empty();
            }

            MqttUnsubAckMessage unsubAck = MqttMessageBuilders.unsubAck()
                                                              .packetId(message.variableHeader().messageId())
                                                              .build();
            return connection.send(unsubAck);
        });
    }
}
