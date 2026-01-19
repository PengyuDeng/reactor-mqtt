/*
 * Copyright 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.mqtt.server;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT 服务端测试
 *
 * @author PengyuDeng
 */
class MqttServerTest {

    private DisposableServer server;
    private static final int TEST_PORT = 11883;

    @BeforeEach
    void setUp() {
        // 每个测试前清理
        if (server != null && !server.isDisposed()) {
            server.disposeNow();
        }
    }

    @AfterEach
    void tearDown() {
        // 每个测试后清理
        if (server != null && !server.isDisposed()) {
            server.disposeNow(Duration.ofSeconds(2));
        }
    }

    @Test
    @Timeout(10)
    void testServerStartAndStop() {
        // 测试服务端能否正常启动和停止
        AtomicBoolean connected = new AtomicBoolean(false);

        server = MqttServer.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .handle(connection -> {
                               connected.set(true);
                               return connection.accept();
                           })
                           .bindNow();

        // 验证服务端已启动
        assertNotNull(server);
        assertFalse(server.isDisposed());
        assertEquals(TEST_PORT, server.port());

        // 停止服务端
        server.disposeNow();
        assertTrue(server.isDisposed());
    }

    @Test
    @Timeout(10)
    void testServerBindReactive() {
        // 测试响应式启动
        AtomicBoolean serverStarted = new AtomicBoolean(false);

        StepVerifier.create(
                        MqttServer.create()
                                  .host("127.0.0.1")
                                  .port(TEST_PORT)
                                  .handle(ServerConnection::accept)
                                  .bind()
                                  .doOnNext(s -> {
                                      server = s;
                                      serverStarted.set(true);
                                  })
                    )
                    .expectNextCount(1)
                    .verifyComplete();

        // 验证服务端已启动
        assertTrue(serverStarted.get());
        assertNotNull(server);
        assertFalse(server.isDisposed());
    }

    @Test
    @Timeout(10)
    void testServerWithCustomConfiguration() {
        // 测试自定义配置
        server = MqttServer.create()
                           .host("0.0.0.0")
                           .port(TEST_PORT)
                           .maxMessageSize(16384)
                           .idleTimeout(Duration.ofSeconds(60))
                           .tcpNoDelay(true)
                           .handle(ServerConnection::accept)
                           .bindNow();

        assertNotNull(server);
        assertFalse(server.isDisposed());
        assertEquals(TEST_PORT, server.port());
    }

    @Test
    @Timeout(10)
    void testMultipleServerInstances() {
        // 测试多个服务端实例（不同端口）
        DisposableServer server1 = MqttServer.create()
                                              .port(TEST_PORT)
                                              .handle(ServerConnection::accept)
                                              .bindNow();

        DisposableServer server2 = MqttServer.create()
                                              .port(TEST_PORT + 1)
                                              .handle(ServerConnection::accept)
                                              .bindNow();

        try {
            assertNotNull(server1);
            assertNotNull(server2);
            assertFalse(server1.isDisposed());
            assertFalse(server2.isDisposed());
            assertEquals(TEST_PORT, server1.port());
            assertEquals(TEST_PORT + 1, server2.port());
        } finally {
            server1.disposeNow();
            server2.disposeNow();
        }
    }

    @Test
    @Timeout(10)
    void testServerDisposeWaitsForCompletion() {
        // 测试服务端dispose等待完成
        server = MqttServer.create()
                           .port(TEST_PORT)
                           .handle(ServerConnection::accept)
                           .bindNow();

        assertNotNull(server);

        // 测试同步dispose
        StepVerifier.create(server.onDispose())
                    .then(() -> server.dispose())
                    .verifyComplete();

        assertTrue(server.isDisposed());
    }

    @Test
    @Timeout(10)
    void testServerHandlerException() {
        // 测试处理器异常情况
        AtomicBoolean handlerCalled = new AtomicBoolean(false);

        server = MqttServer.create()
                           .port(TEST_PORT)
                           .handle(connection -> {
                               handlerCalled.set(true);
                               // 返回reject而不是accept
                               return connection.reject(
                                   io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE
                               );
                           })
                           .bindNow();

        assertNotNull(server);
        assertFalse(server.isDisposed());
    }

    @Test
    @Timeout(10)
    void testServerAddress() {
        // 测试服务端地址
        server = MqttServer.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .handle(connection -> connection.accept())
                           .bindNow();

        assertNotNull(server.address());
        assertTrue(server.address().toString().contains("127.0.0.1"));
        assertEquals(TEST_PORT, server.port());
    }
}
