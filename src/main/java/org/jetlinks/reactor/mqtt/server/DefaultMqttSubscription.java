package org.jetlinks.reactor.mqtt.server;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MQTT 订阅请求实现
 *
 * @author PengyuDeng
 */
public class DefaultMqttSubscription implements MqttSubscription {

    private final MqttSubscribeMessage message;
    private final DefaultServerConnection connection;
    private final AtomicBoolean acknowledged = new AtomicBoolean(false);

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
            if (!acknowledged.compareAndSet(false, true)) {
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
