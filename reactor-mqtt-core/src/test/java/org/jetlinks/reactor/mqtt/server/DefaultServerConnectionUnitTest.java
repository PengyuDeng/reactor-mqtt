package org.jetlinks.reactor.mqtt.server;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.test.StepVerifier;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultServerConnectionUnitTest {

    @Test
    void shouldReleasePublishWhenHandlerThrowsSynchronously() throws Exception {
        DefaultServerConnection connection = newConnection(false);
        connection.handlePublishing(msg -> {
            throw new IllegalStateException("boom");
        });

        MqttPublishMessage message = MqttMessageBuilders.publish()
                                                       .topicName("test/topic")
                                                       .payload(Unpooled.buffer().writeByte(1))
                                                       .qos(MqttQoS.AT_MOST_ONCE)
                                                       .build();

        Method method = DefaultServerConnection.class.getDeclaredMethod("handlePublishSync", MqttPublishMessage.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Mono<Void> result = (Mono<Void>) method.invoke(connection, message);

        StepVerifier.create(result)
                    .expectErrorMessage("boom")
                    .verify();

        assertEquals(1, message.refCnt());

        connection.close().block(Duration.ofSeconds(1));
        ReferenceCountUtil.safeRelease(message);
    }

    @Test
    void shouldInvokeCloseHookWhenKeepAliveTimesOut() throws Exception {
        AtomicReference<Runnable> keepAliveTask = new AtomicReference<>();
        CloseTrackingServerConnection connection = newCloseTrackingConnection(keepAliveTask);

        setLastPingTime(connection, 0L);
        connection.setKeepAliveTimeout(Duration.ofMillis(1)).block(Duration.ofSeconds(1));

        keepAliveTask.get().run();

        assertEquals(1, connection.closeCalls.get());
        StepVerifier.create(connection.onClose())
                    .verifyComplete();
    }

    private DefaultServerConnection newConnection(boolean autoAck) {
        return newConnection(autoAck, null);
    }

    private DefaultServerConnection newConnection(boolean autoAck, AtomicReference<Runnable> keepAliveTask) {
        NettyInbound inbound = (NettyInbound) Proxy.newProxyInstance(
                NettyInbound.class.getClassLoader(),
                new Class[]{NettyInbound.class, Connection.class},
                newInboundHandler(keepAliveTask)
        );
        NettyOutbound outbound = (NettyOutbound) Proxy.newProxyInstance(
                NettyOutbound.class.getClassLoader(),
                new Class[]{NettyOutbound.class},
                (proxy, method, args) -> defaultValue(method.getReturnType())
        );
        return new DefaultServerConnection(inbound, outbound, autoAck);
    }

    private CloseTrackingServerConnection newCloseTrackingConnection(AtomicReference<Runnable> keepAliveTask) {
        NettyInbound inbound = (NettyInbound) Proxy.newProxyInstance(
                NettyInbound.class.getClassLoader(),
                new Class[]{NettyInbound.class, Connection.class},
                newInboundHandler(keepAliveTask)
        );
        NettyOutbound outbound = (NettyOutbound) Proxy.newProxyInstance(
                NettyOutbound.class.getClassLoader(),
                new Class[]{NettyOutbound.class},
                (proxy, method, args) -> defaultValue(method.getReturnType())
        );
        return new CloseTrackingServerConnection(inbound, outbound);
    }

    private InvocationHandler newInboundHandler(AtomicReference<Runnable> keepAliveTask) {
        ScheduledFuture<?> future = newScheduledFuture();
        EventLoop eventLoop = newEventLoop(future, keepAliveTask);
        Channel channel = newChannel(eventLoop);

        return (proxy, method, args) -> switch (method.getName()) {
            case "channel" -> channel;
            case "onDispose" -> proxy;
            case "dispose" -> null;
            case "isDisposed" -> false;
            default -> defaultValue(method.getReturnType());
        };
    }

    private EventLoop newEventLoop(ScheduledFuture<?> future, AtomicReference<Runnable> keepAliveTask) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("scheduleAtFixedRate".equals(method.getName())) {
                if (keepAliveTask != null) {
                    keepAliveTask.set((Runnable) args[0]);
                }
                return future;
            }
            return defaultValue(method.getReturnType());
        };
        return (EventLoop) Proxy.newProxyInstance(
                EventLoop.class.getClassLoader(),
                new Class[]{EventLoop.class},
                handler
        );
    }

    private Channel newChannel(EventLoop eventLoop) {
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "eventLoop" -> eventLoop;
            case "isActive" -> true;
            default -> defaultValue(method.getReturnType());
        };
        return (Channel) Proxy.newProxyInstance(
                Channel.class.getClassLoader(),
                new Class[]{Channel.class},
                handler
        );
    }

    private ScheduledFuture<?> newScheduledFuture() {
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "cancel" -> true;
            case "isCancelled" -> false;
            case "isDone" -> false;
            default -> defaultValue(method.getReturnType());
        };
        return (ScheduledFuture<?>) Proxy.newProxyInstance(
                ScheduledFuture.class.getClassLoader(),
                new Class[]{ScheduledFuture.class},
                handler
        );
    }

    private void setLastPingTime(DefaultServerConnection connection, long lastPingTime) throws Exception {
        java.lang.reflect.Field field = DefaultServerConnection.class.getDeclaredField("lastPingTime");
        field.setAccessible(true);
        field.setLong(connection, lastPingTime);
    }

    private static final class CloseTrackingServerConnection extends DefaultServerConnection {

        private final AtomicInteger closeCalls = new AtomicInteger();

        private CloseTrackingServerConnection(NettyInbound inbound, NettyOutbound outbound) {
            super(inbound, outbound, true);
        }

        @Override
        public Mono<Void> close() {
            closeCalls.incrementAndGet();
            return super.close();
        }
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
