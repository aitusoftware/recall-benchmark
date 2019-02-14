package com.aitusoftware.recall.benchmark;

import com.aitusoftware.recall.map.ByteSequenceMap;
import com.aitusoftware.recall.map.CharSequenceMap;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class SequenceMapBenchmark
{
    private static final int MAP_SIZE = 4096;
    private static final int KEY_COUNT = 2048;
    private static final int KEY_MASK = KEY_COUNT - 1;
    private final Random random = new Random(238429384723L);
    private final CharSequence[] charSequenceKeys = new CharSequence[KEY_COUNT];
    private final ByteBuffer[] byteSequenceKeys = new ByteBuffer[KEY_COUNT];
    private CharSequenceMap charSequenceMap;
    private ByteSequenceMap byteSequenceMap;
    @Param({"10", "100", "200", "1000"})
    private int keyLength = 10;
    private long counter;

    @Setup
    public void setup()
    {
        charSequenceMap = new CharSequenceMap(keyLength, MAP_SIZE, Long.MIN_VALUE);
        byteSequenceMap = new ByteSequenceMap(keyLength, MAP_SIZE, Long.MIN_VALUE);

        for (int i = 0; i < KEY_COUNT; i++)
        {
            charSequenceKeys[i] = randomKey();
            byteSequenceKeys[i] = ByteBuffer.wrap(randomKey().toString().getBytes(StandardCharsets.US_ASCII));
        }
    }

    @Benchmark
    public int storeCharSequence()
    {
        charSequenceMap.put(charSequenceKeys[keyIndex(counter++)], counter);
        return charSequenceMap.size();
    }

    @Benchmark
    public int storeByteSequence()
    {
        byteSequenceMap.put(byteSequenceKeys[keyIndex(counter++)], counter);
        return byteSequenceMap.size();
    }

    private int keyIndex(final long value)
    {
        return (int) (value & KEY_MASK);
    }

    private CharSequence randomKey()
    {
        final StringBuilder builder = new StringBuilder(keyLength);
        for (int i = 0; i < keyLength; i++)
        {
            builder.append((char) ('A' + random.nextInt('Z' - 'A')));
        }
        return builder;
    }
}