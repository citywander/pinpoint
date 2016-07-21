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
import com.navercorp.pinpoint.common.trace.AnnotationKeyFactory;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.trace.ServiceTypeFactory;

import static com.navercorp.pinpoint.common.trace.AnnotationKeyProperty.VIEW_IN_RECORD_SET;
import static com.navercorp.pinpoint.common.trace.ServiceTypeProperty.QUEUE;
import static com.navercorp.pinpoint.common.trace.ServiceTypeProperty.RECORD_STATISTICS;

/**
 * @author Jinkai.Ma
 */
public interface RabbitMQConstants {
    ServiceType RABBITMQ_SERVICE_TYPE = ServiceTypeFactory.of(8300, "RABBITMQ", QUEUE, RECORD_STATISTICS);
    ServiceType RABBITMQ_INTERNAL_SERVICE_TYPE = ServiceTypeFactory.of(8301, "RABBITMQ_INTERNAL", "RABBITMQ");

    AnnotationKey RABBITMQ_BROKER_URL = AnnotationKeyFactory.of(111, "rabbitmq.broker.address", VIEW_IN_RECORD_SET);
    AnnotationKey RABBITMQ_QUEUE_NAME = AnnotationKeyFactory.of(112, "rabbitmq.queue.name", VIEW_IN_RECORD_SET);

    String RABBITMQ_SCOPE = "rabbitmqScope";
    String UNKNOWN_ADDRESS = "Unknown Address";
    String UNKNOWN_QUEUE_NAME = "Unknown Queue";

    String META_DO_NOT_TRACE = "_RABBITMQ_DO_NOT_TRACE";
    String META_TRANSACTION_ID = "_RABBITMQ_TRASACTION_ID";
    String META_SPAN_ID = "_RABBITMQ_SPAN_ID";
    String META_PARENT_SPAN_ID = "_RABBITMQ_PARENT_SPAN_ID";
    String META_PARENT_APPLICATION_NAME = "_RABBITMQ_PARENT_APPLICATION_NAME";
    String META_PARENT_APPLICATION_TYPE = "_RABBITMQ_PARENT_APPLICATION_TYPE";
    String META_FLAGS = "_RABBITMQ_FLAGS";
    String META_HOST = "_RABBITMQ_HOST";

    String FIELD_SETTER_HEADER = "com.navercorp.pinpoint.plugin.rabbitmq.field.setter.HeadersFieldSetter";
}