/*
 * Copyright 2014 NAVER Corp.
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


import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import com.navercorp.pinpoint.bootstrap.context.Trace;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.bootstrap.context.TraceId;
import com.navercorp.pinpoint.bootstrap.interceptor.AroundInterceptor;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.TargetConstructor;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQConstants;

/**
 * @author emeroad
 */
@TargetConstructor({ "byte[]", "org.springframework.amqp.core.MessageProperties" })
public class MessageCreateInterceptor implements AroundInterceptor {

    private final PLogger logger = PLoggerFactory.getLogger(getClass());
    
    private final boolean isDebug = logger.isDebugEnabled();

    private final TraceContext traceContext;

    public MessageCreateInterceptor(TraceContext traceContext) {
        this.traceContext = traceContext;
    }
    @Override
    public void before(Object target, Object[] args) {
        if (isDebug) {
            logger.beforeInterceptor(target, args);
        }

        if (!validate(target, args)) {
            return;
        }

        Trace trace = traceContext.currentRawTraceObject();
        
        // might have been invoked through Channel#basicGet method, in which case, skip
        MessageProperties properties = (MessageProperties) args[1];
        
        if (trace == null) {
            return;
        }

        try {
            Map<String, Object> headers = new HashMap<String, Object>();
            // copy original headers
            if (properties != null && properties.getHeaders() != null) {
                for (String key : properties.getHeaders().keySet()) {
                    headers.put(key, properties.getHeaders().get(key));
                }
            }
            if (trace.canSampled()) {
                TraceId nextId = trace.getTraceId().getNextTraceId();
                properties.setHeader(RabbitMQConstants.META_TRANSACTION_ID, nextId.getTransactionId());
                properties.setHeader(RabbitMQConstants.META_SPAN_ID, Long.toString(nextId.getSpanId()));
                properties.setHeader(RabbitMQConstants.META_PARENT_SPAN_ID, Long.toString(nextId.getParentSpanId()));
                properties.setHeader(RabbitMQConstants.META_FLAGS, Short.toString(nextId.getFlags()));
                properties.setHeader(RabbitMQConstants.META_PARENT_APPLICATION_NAME, traceContext.getApplicationName());
                properties.setHeader(RabbitMQConstants.META_PARENT_APPLICATION_TYPE, traceContext.getServerTypeCode());
            } else {
                headers.put(RabbitMQConstants.META_DO_NOT_TRACE, "1");
            }
        } catch (Throwable t) {
            logger.warn("Failed to before process. {}", t.getMessage(), t);
        }

    }

    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {
        if (isDebug) {
            logger.afterInterceptor(target, args);
        }
/*        if (!validate(target, args)) {
            return;
        }

        Trace trace = traceContext.currentTraceObject();
        if (trace == null) {
            return;
        }

        try {
            SpanEventRecorder recorder = trace.currentSpanEventRecorder();
            recorder.recordApi(descriptor);
            if (throwable == null) {
                AMQChannel channel = (AMQChannel) target;
                AMQConnection connection = channel.getConnection();
                String remoteAddress = RabbitMQUtils.getRemoteAddress(connection);
                recorder.recordEndPoint(remoteAddress);
                recorder.recordAttribute(RabbitMQConstants.RABBITMQ_BROKER_URL, remoteAddress);

                InterceptorScopeInvocation currentInvocation = this.interceptorScope.getCurrentInvocation();
                final String destination = (String) currentInvocation.getAttachment();
                recorder.recordAttribute(AnnotationKey.MESSAGE_QUEUE_URI, connection.toString() + destination);
                recorder.recordDestinationId(destination);
            } else {
                recorder.recordException(throwable);
            }
        } catch (Throwable t) {
            logger.warn("AFTER error. Cause:{}", t.getMessage(), t);
        } finally {
            trace.traceBlockEnd();
        }*/
    }

    private boolean validate(Object target, Object[] args) {
        if (!(target instanceof Message)) {
            return false;
        }
        return true;
    }

}
