package org.jetlinks.reactor.mqtt;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;

public interface MqttConstants {
    interface Message {

        interface Header {
            MqttFixedHeader PINGREQ_HEADER = new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);

            MqttFixedHeader DISCONNECT_HEADER = new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);

            MqttFixedHeader PUBREL_HEADER = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);

            MqttFixedHeader PUBCOMP_HEADER = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);

            MqttFixedHeader PINGRESP_HEADER = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);

            MqttFixedHeader PUBREC_HEADER = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
        }

        MqttMessage DISCONNECT_MESSAGE = new MqttMessage(Header.DISCONNECT_HEADER);

        MqttMessage PING_MESSAGE = new MqttMessage(Header.PINGREQ_HEADER);

        MqttMessage PINGRESP_MESSAGE = new MqttMessage(Header.PINGRESP_HEADER);
    }
}
