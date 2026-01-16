package org.jetlinks.reactor.mqtt;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;

public interface MqttConstants {
    interface Message {

        interface Header {

            MqttFixedHeader PUBREL_HEADER = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);

            MqttFixedHeader PUBCOMP_HEADER = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);

            MqttFixedHeader PUBREC_HEADER = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
        }
    }

    interface Topic {

        char LEVEL_SEPARATOR_CHAR = '/';
        char SINGLE_WILDCARD_CHAR = '+';
        char MULTI_WILDCARD_CHAR = '#';

        String LEVEL_SEPARATOR = String.valueOf(LEVEL_SEPARATOR_CHAR);
        String SINGLE_WILDCARD = String.valueOf(SINGLE_WILDCARD_CHAR);
        String MULTI_WILDCARD = String.valueOf(MULTI_WILDCARD_CHAR);
    }
}
