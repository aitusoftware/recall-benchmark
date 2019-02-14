package com.aitusoftware.recall.benchmark;

import com.aitusoftware.recall.store.BufferStore;
import com.aitusoftware.recall.store.ByteBufferOps;
import com.aitusoftware.recall.store.Store;
import com.aitusoftware.recall.store.UnsafeBufferOps;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ref.BinaryLongReference;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import org.agrona.concurrent.UnsafeBuffer;
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
    private final Store<ByteBuffer> byteBufferStore = new BufferStore<>(
        64, 20_000, ByteBuffer::allocateDirect, new ByteBufferOps());
    private final Store<UnsafeBuffer> unsafeBufferStore = new BufferStore<>(
        64, 20_000, len ->
        new UnsafeBuffer(ByteBuffer.allocateDirect(len)), new UnsafeBufferOps());
    private final OrderByteBufferTranscoder byteBufferTranscoder = new OrderByteBufferTranscoder();
    private final OrderUnsafeBufferTranscoder unsafeBufferTranscoder = new OrderUnsafeBufferTranscoder();
    private final Order[] testData = new Order[TEST_DATA_LENGTH];
    private final long[] ids = new long[IDS_LENGTH];
    private final Random random = new Random(12983719837394L);
    private final BinaryLongReference longRef = new BinaryLongReference();
    private final Order container = new Order();
    private ChronicleMap<LongValue, Order> chronicleMap;

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
            .entries(20_000).averageValue(testData[0])
            .putReturnsNull(true)
            .create();
        longRef.bytesStore(Bytes.allocateDirect(8), 0, 8);

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
        chronicleMap.getUsing(longRef, container);
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
        chronicleMap.getUsing(longRef, container);
    }

    private static int idIndex(final long counter)
    {
        return (int) (counter & IDS_MASK);
    }

    private static int dataIndex(final long counter)
    {
        return (int) (counter & TEST_DATA_MASK);
    }
}