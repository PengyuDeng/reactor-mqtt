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
package org.jetlinks.reactor.mqtt;

/**
 * MQTT 认证信息
 * <p>
 * 包含客户端提供的用户名和密码。
 * </p>
 *
 * @author PengyuDeng
 */
public class MqttAuth {

    private final String username;
    private final String password;

    /**
     * 构造函数
     *
     * @param username 用户名
     * @param password 密码
     */
    public MqttAuth(String username, String password) {
        this.username = username;
        this.password = password;
    }

    /**
     * 创建空认证信息
     */
    public static MqttAuth empty() {
        return new MqttAuth(null, null);
    }

    /**
     * 获取用户名
     *
     * @return 用户名，可能为 null
     */
    public String getUsername() {
        return username;
    }

    /**
     * 获取密码
     *
     * @return 密码，可能为 null
     */
    public String getPassword() {
        return password;
    }

    /**
     * 是否有认证信息
     *
     * @return true 如果有用户名
     */
    public boolean hasAuth() {
        return username != null && !username.isEmpty();
    }

    @Override
    public String toString() {
        if (!hasAuth()) {
            return "MqttAuth{none}";
        }
        return "MqttAuth{" +
                "username='" + username + '\'' +
                ", password='***'" +
                '}';
    }
}
