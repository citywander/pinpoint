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

import com.navercorp.pinpoint.bootstrap.instrument.InstrumentClass;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentException;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentMethod;
import com.navercorp.pinpoint.bootstrap.instrument.Instrumentor;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformCallback;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplate;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplateAware;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPlugin;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPluginSetupContext;

import java.security.ProtectionDomain;

/**
 * @author Jinkai.Ma
 */
public class RabbitMQPlugin implements ProfilerPlugin, TransformTemplateAware {
    private static final String CHANNEL_TRANSMIT_INTERCEPTOR_FQCN = "com.navercorp.pinpoint.plugin.rabbitmq.interceptor.AMQChannelTransmitInterceptor";
    private static final String PUBLISHER_INTERCEPTOR_FQCN = "com.navercorp.pinpoint.plugin.rabbitmq.interceptor.ChannelBasicPublishInterceptor";
    private static final String CONSUMER_INTERCEPTOR_FQCN = "com.navercorp.pinpoint.plugin.rabbitmq.interceptor.ConsumerHandleDeliveryInterceptor";

    private TransformTemplate transformTemplate;

    @Override
    public void setup(ProfilerPluginSetupContext context) {
        this.addPublisherEditors();
        this.addConsumerEditors();
        this.addSpringMessage();
    }

    private void addPublisherEditors() {
        transformTemplate.transform("com.rabbitmq.client.impl.ChannelN", new TransformCallback() {
            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                final InstrumentMethod method = target.getDeclaredMethod("basicPublish", "java.lang.String", "java.lang.String", "boolean", "boolean", "com.rabbitmq.client.AMQP$BasicProperties", "byte[]");
                if (method != null) {
                    method.addInterceptor(PUBLISHER_INTERCEPTOR_FQCN);
                }

                return target.toBytecode();
            }
        });
        transformTemplate.transform("com.rabbitmq.client.impl.AMQChannel", new TransformCallback() {
            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                final InstrumentMethod method = target.getDeclaredMethod("transmit", "com.rabbitmq.client.impl.AMQCommand");
                if (method != null) {
                    method.addInterceptor(CHANNEL_TRANSMIT_INTERCEPTOR_FQCN);
                }

                return target.toBytecode();
            }
        });
        transformTemplate.transform("com.rabbitmq.client.AMQP$BasicProperties", new TransformCallback() {
            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                target.addSetter(RabbitMQConstants.FIELD_SETTER_HEADER, "headers");

                return target.toBytecode();
            }
        });
    }

    private void addConsumerEditors() {
        transformTemplate.transform("com.rabbitmq.client.DefaultConsumer", new TransformCallback() {
            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                final InstrumentMethod method = target.getDeclaredMethod("handleDelivery", "java.lang.String", "com.rabbitmq.client.Envelope", "com.rabbitmq.client.AMQP$BasicProperties", "byte[]");
                if (method != null) {
                    method.addInterceptor(CONSUMER_INTERCEPTOR_FQCN);
                }

                return target.toBytecode();
            }
        });
    }

    @Override
    public void setTransformTemplate(TransformTemplate transformTemplate) {
        this.transformTemplate = transformTemplate;
    }
    
    private void addSpringMessage() {
        transformTemplate.transform("org.springframework.amqp.core.Message", new TransformCallback() {
            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);
                target.addInterceptor("com.navercorp.pinpoint.plugin.rabbitmq.interceptor.MessageCreateInterceptor");
                return target.toBytecode();
            }
        });
    }
    
}