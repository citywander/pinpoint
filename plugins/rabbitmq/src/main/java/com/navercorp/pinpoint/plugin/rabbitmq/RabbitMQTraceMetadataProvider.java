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

package com.navercorp.pinpoint.plugin.rabbitmq;

import com.navercorp.pinpoint.common.trace.AnnotationKey;
import com.navercorp.pinpoint.common.trace.AnnotationKeyMatchers;
import com.navercorp.pinpoint.common.trace.TraceMetadataProvider;
import com.navercorp.pinpoint.common.trace.TraceMetadataSetupContext;

/**
 * @author Jinkai.Ma
 */
public class RabbitMQTraceMetadataProvider implements TraceMetadataProvider {
    @Override
    public void setup(TraceMetadataSetupContext context) {
        context.addServiceType(RabbitMQConstants.RABBITMQ_SERVICE_TYPE, AnnotationKeyMatchers.exact(AnnotationKey.MESSAGE_QUEUE_URI));
        context.addServiceType(RabbitMQConstants.RABBITMQ_INTERNAL_SERVICE_TYPE);

        context.addAnnotationKey(RabbitMQConstants.RABBITMQ_BROKER_URL);
        context.addAnnotationKey(RabbitMQConstants.RABBITMQ_QUEUE_NAME);
    }
}