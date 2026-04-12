package org.jetlinks.reactor.mqtt.server;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.jetlinks.reactor.mqtt.Topic;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;

class DefaultServerReceivedPublishTest {

    @Test
    void shouldReuseInjectedTopic() {
        Topic topic = Topic.of("sensor/room1/temp");
        MqttPublishMessage message = MqttMessageBuilders.publish()
                                                       .topicName(topic.getValue())
                                                       .payload(Unpooled.EMPTY_BUFFER)
                                                       .qos(MqttQoS.AT_MOST_ONCE)
                                                       .build();

        DefaultServerReceivedPublish publishing = new DefaultServerReceivedPublish(message, topic, null);

        assertSame(topic, publishing.topic());
        assertSame(topic, publishing.topic());
    }
}
