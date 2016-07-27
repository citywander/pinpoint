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

package com.navercorp.pinpoint.plugin.rabbitmq.interceptor;

import com.navercorp.pinpoint.bootstrap.context.MethodDescriptor;
import com.navercorp.pinpoint.bootstrap.context.SpanEventRecorder;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.bootstrap.interceptor.SpanEventSimpleAroundInterceptorForPlugin;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.Scope;
import com.navercorp.pinpoint.bootstrap.util.StringUtils;
import com.navercorp.pinpoint.common.trace.AnnotationKey;
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQConstants;
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.AMQConnection;

/**
 * @author HyunGil Jeong
 */
@Scope(value = RabbitMQConstants.RABBITMQ_SCOPE)
public class ChannelBasicGetInterceptor extends SpanEventSimpleAroundInterceptorForPlugin {

    public  ChannelBasicGetInterceptor(TraceContext traceContext, MethodDescriptor descriptor) {
        super(traceContext, descriptor);
    }

    @Override
    protected void doInBeforeTrace(SpanEventRecorder recorder, Object target, Object[] args) {
        // Do nothing
    }

    @Override
    protected void doInAfterTrace(SpanEventRecorder recorder, Object target, Object[] args, Object result, Throwable throwable) {
        if (validate(target, args, result)) {
            recorder.recordServiceType(RabbitMQConstants.RABBITMQ_SERVICE_TYPE);

            AMQConnection connection = (AMQConnection) ((Channel) target).getConnection();
            String remoteAddress = RabbitMQUtils.getRemoteAddress(connection);
            recorder.recordEndPoint(remoteAddress);
            recorder.recordAttribute(RabbitMQConstants.RABBITMQ_BROKER_URL, remoteAddress);

            GetResponse response = (GetResponse) result;
            Envelope envelope = response.getEnvelope();
            String destination = RabbitMQUtils.createDestination(envelope.getExchange(), envelope.getRoutingKey());
            recorder.recordAttribute(AnnotationKey.MESSAGE_QUEUE_URI, connection.toString() + destination);
            recorder.recordDestinationId(destination);

            String queueName = StringUtils.defaultString((String) args[0], RabbitMQConstants.UNKNOWN_QUEUE_NAME);
            recorder.recordAttribute(RabbitMQConstants.RABBITMQ_QUEUE_NAME, queueName);
        } else {
            recorder.recordServiceType(RabbitMQConstants.RABBITMQ_INTERNAL_SERVICE_TYPE);
        }

        recorder.recordApi(getMethodDescriptor());
        if (throwable != null) {
            recorder.recordException(throwable);
        }
    }

    private boolean validate(Object target, Object[] args, Object result) {
        if (!(target instanceof Channel)) {
            return false;
        }
        if (args == null || args.length < 1) {
            return false;
        }
        if (!(((Channel) target).getConnection() instanceof AMQConnection)) {
            return false;
        }
        if (!(args[0] instanceof String)) {
            return false;
        }
        if (!(result instanceof GetResponse)) {
            return false;
        }
        return true;
    }

}