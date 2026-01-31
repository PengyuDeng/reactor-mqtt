package org.jetlinks.reactor.mqtt;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.time.Duration;

/**
 * MQTT 协议常量定义
 *
 * @author PengyuDeng
 */
public interface MqttConstants {

    /**
     * MQTT 消息头常量
     *
     * <p>预定义的 MqttFixedHeader 常量，避免重复创建对象。</p>
     */
    interface MessageHeader {

        /**
         * PUBREC 消息头（QoS 2 第一阶段确认）
         */
        MqttFixedHeader PUBREC_HEADER = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);

        /**
         * PUBREL 消息头（QoS 2 第二阶段释放）
         */
        MqttFixedHeader PUBREL_HEADER = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);

        /**
         * PUBCOMP 消息头（QoS 2 第三阶段完成）
         */
        MqttFixedHeader PUBCOMP_HEADER = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);
    }

    /**
     * MQTT 主题相关常量
     */
    interface Topic {

        char LEVEL_SEPARATOR_CHAR = '/';
        char SINGLE_WILDCARD_CHAR = '+';
        char MULTI_WILDCARD_CHAR = '#';

        String LEVEL_SEPARATOR = String.valueOf(LEVEL_SEPARATOR_CHAR);
        String SINGLE_WILDCARD = String.valueOf(SINGLE_WILDCARD_CHAR);
        String MULTI_WILDCARD = String.valueOf(MULTI_WILDCARD_CHAR);
    }

    /**
     * 时间常量
     */
    interface Time {
        Duration TEN_SECONDS = Duration.ofSeconds(10);
        Duration THIRTY_SECONDS = Duration.ofSeconds(30);
    }
}
