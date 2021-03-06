package com.navercorp.pinpoint.profiler.sender.planer;

import com.navercorp.pinpoint.common.Version;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.util.JvmUtils;
import com.navercorp.pinpoint.common.util.SystemPropertyKey;
import com.navercorp.pinpoint.profiler.AgentInformation;
import com.navercorp.pinpoint.profiler.context.DefaultTraceId;
import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanChunkFactory;
import com.navercorp.pinpoint.profiler.context.SpanEvent;
import com.navercorp.pinpoint.profiler.sender.HeaderTBaseSerializerPoolFactory;
import com.navercorp.pinpoint.profiler.sender.PartitionedByteBufferLocator;
import com.navercorp.pinpoint.profiler.sender.SpanStreamSendData;
import com.navercorp.pinpoint.profiler.sender.SpanStreamSendDataFactory;
import com.navercorp.pinpoint.profiler.sender.SpanStreamSendDataSerializer;
import com.navercorp.pinpoint.profiler.util.ObjectPool;
import com.navercorp.pinpoint.thrift.dto.TSpanEvent;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseDeserializer;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseDeserializerFactory;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseSerializer;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseSerializerFactory;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SpanStreamSendDataPlanerTest {

    private static SpanChunkFactory spanChunkFactory;

    private static ObjectPool<HeaderTBaseSerializer> objectPool;

    @BeforeClass
    public static void setUp() {
        AgentInformation agentInformation = new AgentInformation("agentId", "applicationName", 0, 0, "machineName", "127.0.0.1", ServiceType.STAND_ALONE,
                JvmUtils.getSystemProperty(SystemPropertyKey.JAVA_VERSION), Version.VERSION);

        HeaderTBaseSerializerPoolFactory serializerFactory = new HeaderTBaseSerializerPoolFactory(true, 1000, true);
        objectPool = new ObjectPool<HeaderTBaseSerializer>(serializerFactory, 16);

        spanChunkFactory = new SpanChunkFactory(agentInformation);
    }

    @Test
    public void spanStreamSendDataPlanerTest() throws Exception {
        int spanEventSize = 10;

        SpanStreamSendDataSerializer serializer = new SpanStreamSendDataSerializer();

        HeaderTBaseSerializerFactory headerTBaseSerializerFactory = new HeaderTBaseSerializerFactory();

        List<SpanEvent> originalSpanEventList = createSpanEventList(spanEventSize);
        Span span = createSpan(originalSpanEventList);

        PartitionedByteBufferLocator partitionedByteBufferLocator = serializer.serializeSpanStream(headerTBaseSerializerFactory.createSerializer(), span);
        SpanStreamSendDataFactory factory = new SpanStreamSendDataFactory(100, 50, objectPool);
        List<TSpanEvent> spanEventList = getSpanEventList(partitionedByteBufferLocator, factory);

        partitionedByteBufferLocator = serializer.serializeSpanStream(headerTBaseSerializerFactory.createSerializer(), span);
        factory = new SpanStreamSendDataFactory(objectPool);
        List<TSpanEvent> spanEventList2 = getSpanEventList(partitionedByteBufferLocator, factory);

        Assert.assertEquals(spanEventSize, spanEventList.size());
        Assert.assertEquals(spanEventSize, spanEventList2.size());
    }

    private Span createSpan(List<SpanEvent> spanEventList) {
        DefaultTraceId traceId = new DefaultTraceId("test", 0, 1);
        Span span = new Span();

        for (SpanEvent spanEvent : spanEventList) {
            span.addToSpanEventList(spanEvent);
        }

        span.setAgentId("agentId");
        span.recordTraceId(traceId);
        return span;
    }

    private List<SpanEvent> createSpanEventList(int size) throws InterruptedException {
        Span span = new Span();

        List<SpanEvent> spanEventList = new ArrayList<SpanEvent>(size);
        for (int i = 0; i < size; i++) {
            SpanEvent spanEvent = new SpanEvent(span);
            spanEvent.markStartTime();
            Thread.sleep(1);
            spanEvent.markAfterTime();

            spanEventList.add(spanEvent);
        }

        return spanEventList;
    }

    private List<TSpanEvent> getSpanEventList(PartitionedByteBufferLocator partitionedByteBufferLocator, SpanStreamSendDataFactory factory) throws Exception {
        List<TSpanEvent> spanEventList = new ArrayList<TSpanEvent>();

        SpanChunkStreamSendDataPlaner planer = new SpanChunkStreamSendDataPlaner(partitionedByteBufferLocator, factory);
        Iterator<SpanStreamSendData> iterator = planer.getSendDataIterator();
        while (iterator.hasNext()) {
            SpanStreamSendData data = iterator.next();

            ByteBuffer[] sendBuffers = data.getSendBuffers();
            byte[] relatedBuffer = getSpanRelatedBuffer(sendBuffers);

            List<TSpanEvent> result = deserialize(relatedBuffer);

            for (TSpanEvent spanEvent : result) {
                spanEventList.add(spanEvent);
            }
        }

        return spanEventList;
    }

    private byte[] getSpanRelatedBuffer(ByteBuffer[] buffers) {
        int totalBufferSize = 0;

        for (ByteBuffer buffer : buffers) {
            totalBufferSize += buffer.remaining();
        }

        byte[] relatedBuffer = new byte[totalBufferSize];

        int offset = 0;
        for (ByteBuffer buffer : buffers) {
            int bufferlength = buffer.remaining();

            buffer.get(relatedBuffer, offset, bufferlength);
            offset += bufferlength;
        }

        return relatedBuffer;
    }


    private List<TSpanEvent> deserialize(byte[] data) throws TException {
        ByteBuffer bb = ByteBuffer.wrap(data);

        bb.get();
        bb.get();
        int chunkSize = bb.get();

        List<TSpanEvent> eventList = new ArrayList<TSpanEvent>();

        for (int i = 0; i < chunkSize; i++) {
            short componentSize = bb.getShort();
            byte[] component = new byte[componentSize];
            bb.get(component);

            HeaderTBaseDeserializer deserialize = new HeaderTBaseDeserializerFactory().createDeserializer();
            List<TBase<?, ?>> value = deserialize.deserializeList(component);

            for (int j = 0; j < value.size(); j++) {
                TBase tbase = value.get(j);

                if (tbase instanceof TSpanEvent) {
                    eventList.add((TSpanEvent) tbase);
                } else {
                }
            }

        }
        return eventList;
    }


}
