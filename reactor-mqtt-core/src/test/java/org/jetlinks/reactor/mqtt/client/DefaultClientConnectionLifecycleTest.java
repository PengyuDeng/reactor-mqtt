package org.jetlinks.reactor.mqtt.client;

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;
import reactor.test.StepVerifier;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultClientConnectionLifecycleTest {

    @Test
    void shouldCompleteOnCloseEvenWhenObservedAfterClose() {
        DefaultClientConnection connection = newConnection(ReconnectStrategy.none(), () -> TcpClient.create());

        connection.close().block(Duration.ofSeconds(1));

        StepVerifier.create(connection.onClose())
                    .verifyComplete();
    }

    @Test
    void shouldCancelPendingReconnectWhenClosed() throws Exception {
        AtomicInteger supplierCalls = new AtomicInteger();
        Supplier<TcpClient> tcpClientSupplier = () -> {
            supplierCalls.incrementAndGet();
            return TcpClient.create().host("127.0.0.1").port(1);
        };

        DefaultClientConnection connection = newConnection(
                ReconnectStrategy.fixedDelay(Duration.ofMillis(200)),
                tcpClientSupplier
        );

        Method attemptReconnect = DefaultClientConnection.class.getDeclaredMethod("attemptReconnect");
        attemptReconnect.setAccessible(true);
        attemptReconnect.invoke(connection);

        connection.close().block(Duration.ofSeconds(1));
        Thread.sleep(350);

        assertEquals(0, supplierCalls.get());
        StepVerifier.create(connection.onClose())
                    .verifyComplete();
    }

    private DefaultClientConnection newConnection(ReconnectStrategy reconnectStrategy,
                                                  Supplier<TcpClient> tcpClientSupplier) {
        MqttClientConfig config = new MqttClientConfig();
        config.setReconnectStrategy(reconnectStrategy);
        return new DefaultClientConnection(newFakeNettyConnection(), config, tcpClientSupplier);
    }

    private Connection newFakeNettyConnection() {
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "dispose" -> null;
            case "isDisposed" -> false;
            case "channel" -> null;
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
