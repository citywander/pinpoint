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
import com.navercorp.pinpoint.bootstrap.context.Trace;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.bootstrap.context.TraceId;
import com.navercorp.pinpoint.bootstrap.interceptor.AroundInterceptor;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.Name;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.Scope;
import com.navercorp.pinpoint.bootstrap.interceptor.scope.ExecutionPolicy;
import com.navercorp.pinpoint.bootstrap.interceptor.scope.InterceptorScope;
import com.navercorp.pinpoint.bootstrap.interceptor.scope.InterceptorScopeInvocation;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.util.StringUtils;
import com.navercorp.pinpoint.common.trace.AnnotationKey;
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQConstants;
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQUtils;
import com.navercorp.pinpoint.plugin.rabbitmq.field.setter.HeadersFieldSetter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.impl.AMQChannel;
import com.rabbitmq.client.impl.AMQCommand;
import com.rabbitmq.client.impl.AMQConnection;

import java.util.HashMap;
import java.util.Map;

/**
 * @author HyunGil Jeong
 */
@Scope(value = RabbitMQConstants.RABBITMQ_SCOPE, executionPolicy = ExecutionPolicy.INTERNAL)
public class AMQChannelTransmitInterceptor implements AroundInterceptor {

    private final PLogger logger = PLoggerFactory.getLogger(getClass());
    private final boolean isDebug = logger.isDebugEnabled();

    private final TraceContext traceContext;
    private final MethodDescriptor descriptor;
    private final InterceptorScope interceptorScope;

    public AMQChannelTransmitInterceptor(TraceContext traceContext, MethodDescriptor descriptor, @Name(RabbitMQConstants.RABBITMQ_SCOPE) InterceptorScope interceptorScope) {
        this.traceContext = traceContext;
        this.descriptor = descriptor;
        this.interceptorScope = interceptorScope;
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
        if (trace == null) {
            return;
        }

        try {

            InterceptorScopeInvocation currentInvocation = this.interceptorScope.getCurrentInvocation();
            final String destination = (String) currentInvocation.getAttachment();
            // might have been invoked through Channel#basicGet method, in which case, skip
            if (StringUtils.isEmpty(destination)) {
                return;
            }
            AMQCommand command = (AMQCommand) args[0];
            AMQP.BasicProperties properties = (AMQP.BasicProperties) command.getContentHeader();
            Map<String, Object> headers = new HashMap<String, Object>();
            // copy original headers
            if (properties != null && properties.getHeaders() != null) {
                for (String key : properties.getHeaders().keySet()) {
                    headers.put(key, properties.getHeaders().get(key));
                }
            }

            if (trace.canSampled()) {
                SpanEventRecorder recorder = trace.traceBlockBegin();
                recorder.recordServiceType(RabbitMQConstants.RABBITMQ_SERVICE_TYPE);

                TraceId nextId = trace.getTraceId().getNextTraceId();

                recorder.recordNextSpanId(nextId.getSpanId());

                headers.put(RabbitMQConstants.META_TRANSACTION_ID, nextId.getTransactionId());
                headers.put(RabbitMQConstants.META_SPAN_ID, Long.toString(nextId.getSpanId()));
                headers.put(RabbitMQConstants.META_PARENT_SPAN_ID, Long.toString(nextId.getParentSpanId()));
                headers.put(RabbitMQConstants.META_FLAGS, Short.toString(nextId.getFlags()));
                headers.put(RabbitMQConstants.META_PARENT_APPLICATION_NAME, traceContext.getApplicationName());
                headers.put(RabbitMQConstants.META_PARENT_APPLICATION_TYPE, traceContext.getServerTypeCode());
                headers.put(RabbitMQConstants.META_HOST, destination);
            } else {
                headers.put(RabbitMQConstants.META_DO_NOT_TRACE, "1");
            }
            ((HeadersFieldSetter) properties)._$PINPOINT$_setHeaders(headers);
        } catch (Throwable t) {
            logger.warn("Failed to before process. {}", t.getMessage(), t);
        }

    }

    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {
        if (isDebug) {
            logger.afterInterceptor(target, args);
        }
        if (!validate(target, args)) {
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
        }
    }

    private boolean validate(Object target, Object[] args) {
        if (!(target instanceof AMQChannel)) {
            return false;
        }
        if (args == null || args.length < 1) {
            return false;
        }
        if (!(args[0] instanceof AMQCommand)) {
            return false;
        }
        if (!(((AMQChannel) target).getConnection() instanceof AMQConnection)) {
            return false;
        }
        if (!(((AMQCommand) args[0]).getContentHeader() instanceof AMQP.BasicProperties)) {
            return false;
        }
        if (!(((AMQCommand) args[0]).getContentHeader() instanceof HeadersFieldSetter)) {
            return false;
        }
        return true;
    }
}