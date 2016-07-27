/*
 * Copyright 2016 Naver Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.plugin.rabbitmq;

import com.navercorp.pinpoint.bootstrap.util.StringUtils;
import com.rabbitmq.client.impl.AMQConnection;

/**
 * @author HyunGil Jeong
 */
public class RabbitMQUtils {

    private RabbitMQUtils() {}

    public static String getLocalAddress(AMQConnection connection) {
        if (connection == null || connection.getLocalAddress() == null || connection.getLocalAddress().getHostAddress() == null) {
            return RabbitMQConstants.UNKNOWN_ADDRESS;
        }
        return connection.getLocalAddress().getHostAddress() + ":" + connection.getLocalPort();
    }

    public static String getRemoteAddress(AMQConnection connection) {
        if (connection == null || connection.getAddress() == null || connection.getAddress().getHostAddress() == null) {
            return RabbitMQConstants.UNKNOWN_ADDRESS;
        }
        return connection.getAddress().getHostAddress() + ":" + connection.getPort();
    }

    public static String createDestination(String exchange, String routingKey) {
        return StringUtils.defaultString(exchange, "") + "/" + StringUtils.defaultString(routingKey, "");
    }
}