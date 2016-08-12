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
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQConstants;
import com.navercorp.pinpoint.bootstrap.context.MethodDescriptor;
import com.navercorp.pinpoint.bootstrap.context.SpanEventRecorder;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.Scope;
import com.navercorp.pinpoint.plugin.rabbitmq.RabbitMQUtils;

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
    protected void doInBeforeTrace(SpanEventRecorder recorder, Object target, Object[] args) {
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
}