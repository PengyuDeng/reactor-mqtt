package org.jetlinks.reactor.mqtt.server;

import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MQTT 取消订阅请求实现
 *
 * @author PengyuDeng
 */
public class DefaultMqttUnsubscription implements MqttUnsubscription {

    private final MqttUnsubscribeMessage message;
    private final DefaultServerConnection connection;
    private final AtomicBoolean acknowledged = new AtomicBoolean(false);

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
            if (!acknowledged.compareAndSet(false, true)) {
                return Mono.empty();
            }

            MqttUnsubAckMessage unsubAck = MqttMessageBuilders.unsubAck()
                                                              .packetId(message.variableHeader().messageId())
                                                              .build();
            return connection.send(unsubAck);
        });
    }
}
