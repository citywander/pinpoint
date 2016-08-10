/*
 * Copyright 2016 NAVER Corp.
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

import com.navercorp.pinpoint.bootstrap.interceptor.SpanEventSimpleAroundInterceptorForPlugin;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.Name;
import com.navercorp.pinpoint.bootstrap.interceptor.scope.InterceptorScope;
import com.navercorp.pinpoint.bootstrap.util.NumberUtils;
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQConstants;

import java.util.Map;

import com.navercorp.pinpoint.bootstrap.context.MethodDescriptor;
import com.navercorp.pinpoint.bootstrap.context.SpanEventRecorder;
import com.navercorp.pinpoint.bootstrap.context.SpanId;
import com.navercorp.pinpoint.bootstrap.context.Trace;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.bootstrap.context.TraceId;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.Scope;
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQUtils;
import com.rabbitmq.client.AMQP;

/**
 * @author Jinkai.Ma
 */
@Scope(value = RabbitMQConstants.RABBITMQ_SCOPE)
public class ChannelBasicPublishInterceptor extends SpanEventSimpleAroundInterceptorForPlugin {

    private final InterceptorScope interceptorScope;

    public ChannelBasicPublishInterceptor(TraceContext traceContext, MethodDescriptor descriptor, @Name(RabbitMQConstants.RABBITMQ_SCOPE) InterceptorScope interceptorScope) {
        super(traceContext, descriptor);
        this.interceptorScope = interceptorScope;
    }
    
    @Override
    protected void prepareBeforeTrace(Object target, Object[] args) {
        this.setCurrentInvocation(target, args);
        final Trace trace = traceContext.currentTraceObject();
        if(trace == null){
            AMQP.BasicProperties properties = (AMQP.BasicProperties)args[4];
            this.createTrace(properties);
        }
    }
    
    @Override
    protected void doInBeforeTrace(SpanEventRecorder recorder, Object target, Object[] args) {
        this.setCurrentInvocation(target, args);
    }
    
    private void setCurrentInvocation(Object target, Object[] args){
        String routingKey = (String) args[1];
        String exchange = (String) args[0];
        final String destination = RabbitMQUtils.createDestination(exchange, routingKey);
        this.interceptorScope.getCurrentInvocation().setAttachment(destination);
    }

    @Override
    protected void doInAfterTrace(SpanEventRecorder recorder, Object target, Object[] args, Object result, Throwable throwable) {
        recorder.recordServiceType(RabbitMQConstants.RABBITMQ_INTERNAL_SERVICE_TYPE);
        recorder.recordApi(getMethodDescriptor());
        if (throwable != null) {
            recorder.recordException(throwable);
        }
    }
    
    private Trace createTrace(AMQP.BasicProperties properties) {
        if(properties == null){
            return null;
        }
        Map<String, Object> headers = properties.getHeaders();
        // If this transaction is not traceable, mark as disabled.
        if (headers.get(RabbitMQConstants.META_DO_NOT_TRACE) != null) {
            return traceContext.disableSampling();
        }

        Object transactionId = headers.get(RabbitMQConstants.META_TRANSACTION_ID);
        // If there's no trasanction id, a new trasaction begins here.
        if (transactionId == null) {
            return traceContext.newTraceObject();
        }

        // otherwise, continue tracing with given data.
        long parentSpanID = NumberUtils.parseLong(headers.get(RabbitMQConstants.META_PARENT_SPAN_ID).toString(), SpanId.NULL);
        long spanID = NumberUtils.parseLong(headers.get(RabbitMQConstants.META_SPAN_ID).toString(), SpanId.NULL);
        short flags = NumberUtils.parseShort(headers.get(RabbitMQConstants.META_FLAGS).toString(), (short) 0);
        TraceId traceId = traceContext.createTraceId(transactionId.toString(), parentSpanID, spanID, flags);

        return traceContext.continueTraceObject(traceId);
    }
    
}