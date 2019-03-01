package com.aitusoftware.recall.benchmark;

import com.aitusoftware.recall.persistence.AsciiCharSequence;
import com.aitusoftware.recall.store.BufferStore;
import com.aitusoftware.recall.store.ByteBufferOps;
import com.aitusoftware.recall.store.Store;
import com.aitusoftware.recall.store.UnsafeBufferOps;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ref.BinaryLongReference;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class StoreBenchmark
{
    private static final int TEST_DATA_LENGTH = 128;
    private static final int TEST_DATA_MASK = TEST_DATA_LENGTH - 1;
    private static final int IDS_LENGTH = 16384;
    private static final int SAMPLE_POINT = IDS_LENGTH / 5;
    private static final int IDS_MASK = IDS_LENGTH - 1;
    private static final int MAX_RECORD_LENGTH = 64;
    private static final int ENTRIES = 20_000;
    private final Store<ByteBuffer> byteBufferStore = new BufferStore<>(
            MAX_RECORD_LENGTH, ENTRIES, ByteBuffer::allocateDirect, new ByteBufferOps());
    private final Store<UnsafeBuffer> unsafeBufferStore = new BufferStore<>(
            MAX_RECORD_LENGTH, ENTRIES, len ->
        new UnsafeBuffer(ByteBuffer.allocateDirect(len)), new UnsafeBufferOps());
    private final OrderByteBufferTranscoder byteBufferTranscoder = new OrderByteBufferTranscoder();
    private final OrderUnsafeBufferTranscoder unsafeBufferTranscoder = new OrderUnsafeBufferTranscoder();
    private final Order[] testData = new Order[TEST_DATA_LENGTH];
    private final long[] ids = new long[IDS_LENGTH];
    private final Random random = new Random(12983719837394L);
    private final BinaryLongReference longRef = new BinaryLongReference();
    private final Order container = new Order();
    private ChronicleMap<LongValue, Order> chronicleMap;
    private OHCache<LongValue, Order> ohCache;

    private long counter = 0;

    @Setup
    public void setup()
    {
        for (int i = 0; i < TEST_DATA_LENGTH; i++)
        {
            final Order order = new Order();
            testData[i] = order;
            order.set(0, random.nextDouble(), random.nextDouble(), random.nextLong(),
                random.nextInt(), random.nextLong(), "SYM_" + ((char) ('A' + random.nextInt(20))));
        }
        for (int i = 0; i < IDS_LENGTH; i++)
        {
            ids[i] = random.nextLong();
        }
        chronicleMap = ChronicleMap.of(LongValue.class, Order.class)
            .entries(ENTRIES).averageValue(testData[0])
            .putReturnsNull(true)
            .create();
        longRef.bytesStore(Bytes.allocateDirect(8), 0, 8);
        ohCache = OHCacheBuilder.<LongValue, Order>newBuilder()
                .keySerializer(new LongValueCacheSerializer())
                .valueSerializer(new OrderCacheSerialiser())
                .fixedEntrySize(Long.BYTES, MAX_RECORD_LENGTH)
                .capacity(ENTRIES)
                .chunkSize(128)
                .unlocked(true)
                .build();

        populateMaps();
    }

    private void populateMaps()
    {
        for (int i = 0; i < ids.length; i++)
        {
            final Order testDatum = testData[dataIndex(i)];
            testDatum.setId(ids[idIndex(i)]);
            byteBufferStore.store(byteBufferTranscoder, testDatum, byteBufferTranscoder);
            unsafeBufferStore.store(unsafeBufferTranscoder, testDatum, unsafeBufferTranscoder);
            longRef.setValue(testDatum.getId());
            chronicleMap.put(longRef, testDatum);
            ohCache.put(longRef, testDatum);
        }
    }

    @Benchmark
    public long storeEntryByteBuffer()
    {
        final Order testDatum = testData[dataIndex(counter)];
        testDatum.setId(ids[idIndex(counter)]);
        counter++;
        byteBufferStore.store(byteBufferTranscoder, testDatum, byteBufferTranscoder);
        return byteBufferStore.size();
    }

    @Benchmark
    public long storeEntryUnsafeBuffer()
    {
        final Order testDatum = testData[dataIndex(counter)];
        testDatum.setId(ids[idIndex(counter)]);
        counter++;
        unsafeBufferStore.store(unsafeBufferTranscoder, testDatum, unsafeBufferTranscoder);
        return unsafeBufferStore.size();
    }

    @Benchmark
    public long storeEntryChronicleMap()
    {
        final Order testDatum = testData[dataIndex(counter)];
        testDatum.setId(ids[idIndex(counter)]);
        counter++;
        longRef.setValue(testDatum.getId());
        chronicleMap.put(longRef, testDatum);
        return chronicleMap.size();
    }

    @Benchmark
    public long storeEntryOHCMap()
    {
        final Order testDatum = testData[dataIndex(counter)];
        testDatum.setId(ids[idIndex(counter)]);
        counter++;
        longRef.setValue(testDatum.getId());
        ohCache.put(longRef, testDatum);
        return ohCache.size();
    }

    @Benchmark
    public void getSingleEntryByteBuffer(final Blackhole bh)
    {
        bh.consume(byteBufferStore.load(ids[idIndex(SAMPLE_POINT)], byteBufferTranscoder, container));
    }

    @Benchmark
    public void getSingleEntryUnsafeBuffer(final Blackhole bh)
    {
        bh.consume(unsafeBufferStore.load(ids[idIndex(SAMPLE_POINT)], unsafeBufferTranscoder, container));
    }

    @Benchmark
    public void getSingleEntryChronicleMap(final Blackhole bh)
    {
        longRef.setValue(ids[idIndex(SAMPLE_POINT)]);
        bh.consume(chronicleMap.getUsing(longRef, container));
    }

    @Benchmark
    public void getSingleEntryOHCMap(final Blackhole bh)
    {
        longRef.setValue(ids[idIndex(SAMPLE_POINT)]);
        bh.consume(ohCache.get(longRef));
    }

    @Benchmark
    public void getRandomEntryByteBuffer(final Blackhole bh)
    {
        bh.consume(byteBufferStore.load(ids[idIndex(counter++)], byteBufferTranscoder, container));
    }

    @Benchmark
    public void getRandomEntryUnsafeBuffer(final Blackhole bh)
    {
        bh.consume(unsafeBufferStore.load(ids[idIndex(counter++)], unsafeBufferTranscoder, container));
    }

    @Benchmark
    public void getRandomEntryChronicleMap(final Blackhole bh)
    {
        longRef.setValue(ids[idIndex(counter++)]);
        bh.consume(chronicleMap.getUsing(longRef, container));
    }

    @Benchmark
    public void getRandomEntryOHCMap(final Blackhole bh)
    {
        longRef.setValue(ids[idIndex(counter++)]);
        bh.consume(ohCache.get(longRef));
    }

    private static int idIndex(final long counter)
    {
        return (int) (counter & IDS_MASK);
    }

    private static int dataIndex(final long counter)
    {
        return (int) (counter & TEST_DATA_MASK);
    }

    private static class LongValueCacheSerializer implements CacheSerializer<LongValue>
    {
        @Override
        public void serialize(final LongValue value, final ByteBuffer buf)
        {
            buf.putLong(value.getValue());
        }

        @Override
        public LongValue deserialize(final ByteBuffer buf)
        {
            final BinaryLongReference longRef = new BinaryLongReference();
            longRef.bytesStore(Bytes.allocateDirect(8), 0, 8);
            return longRef;
        }

        @Override
        public int serializedSize(LongValue value)
        {
            return Long.BYTES;
        }
    }

    private class OrderCacheSerialiser implements CacheSerializer<Order>
    {
        private static final int SESSION_ID_OFFSET = Long.BYTES;
        private static final int TIMESTAMP_OFFSET = (2 * Long.BYTES);
        private static final int QUANTITY_OFFSET = (3 * Long.BYTES);
        private static final int PRICE_OFFSET = (4 * Long.BYTES);
        private static final int VENUE_ID_OFFSET = (5 * Long.BYTES);
        private static final int SYMBOL_LENGTH_OFFSET = (5 * Long.BYTES) + Integer.BYTES;
        private static final int SYMBOL_CHAR_BASE_OFFSET = (5 * Long.BYTES) + (2 * Integer.BYTES);

        @Override
        public void serialize(Order value, ByteBuffer buffer)
        {
            final int offset = buffer.position();
            buffer.putLong(offset, value.getId());
            buffer.putLong(offset + SESSION_ID_OFFSET, value.getSessionId());
            buffer.putLong(offset + TIMESTAMP_OFFSET, value.getTimestamp());
            buffer.putLong(offset + QUANTITY_OFFSET, Double.doubleToRawLongBits(value.getQuantity()));
            buffer.putLong(offset + PRICE_OFFSET, Double.doubleToRawLongBits(value.getPrice()));
            buffer.putInt(offset + VENUE_ID_OFFSET, value.getVenueId());
            final int length = value.getSymbol().length();
            buffer.putInt(offset + SYMBOL_LENGTH_OFFSET, length);
            for (int i = 0; i < length; i++)
            {
                buffer.putChar(offset + SYMBOL_CHAR_BASE_OFFSET + (i * Character.BYTES),
                        value.getSymbol().charAt(i));
            }
        }

        @Override
        public Order deserialize(ByteBuffer buffer)
        {
            final Order container = new Order();
            final int offset = buffer.position();
            container.setId(buffer.getLong(offset));
            container.setSessionId(buffer.getLong(offset + SESSION_ID_OFFSET));
            container.setTimestamp(buffer.getLong(offset + TIMESTAMP_OFFSET));
            container.setQuantity(Double.longBitsToDouble(buffer.getLong(offset + QUANTITY_OFFSET)));
            container.setPrice(Double.longBitsToDouble(buffer.getLong(offset + PRICE_OFFSET)));
            container.setVenueId(buffer.getInt(offset + VENUE_ID_OFFSET));
            final int symbolLength = buffer.getInt(offset + SYMBOL_LENGTH_OFFSET);
            final AsciiCharSequence symbolSequence = container.getSymbolSequence();
            symbolSequence.reset();
            for (int i = 0; i < symbolLength; i++)
            {
                symbolSequence.append(buffer.getChar(offset + SYMBOL_CHAR_BASE_OFFSET + (i * Character.BYTES)));
            }

            return container;
        }

        @Override
        public int serializedSize(Order value)
        {
            return MAX_RECORD_LENGTH;
        }
    }
}