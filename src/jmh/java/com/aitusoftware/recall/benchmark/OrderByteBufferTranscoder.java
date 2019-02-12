package com.aitusoftware.recall.benchmark;

import com.aitusoftware.recall.persistence.AsciiCharSequence;
import com.aitusoftware.recall.persistence.Decoder;
import com.aitusoftware.recall.persistence.Encoder;
import com.aitusoftware.recall.persistence.IdAccessor;

import java.nio.ByteBuffer;

public final class OrderByteBufferTranscoder implements Encoder<ByteBuffer, Order>,
    Decoder<ByteBuffer, Order>, IdAccessor<Order>
{

    private static final int SESSION_ID_OFFSET = Long.BYTES;
    private static final int TIMESTAMP_OFFSET = (2 * Long.BYTES);
    private static final int QUANTITY_OFFSET = (3 * Long.BYTES);
    private static final int PRICE_OFFSET = (4 * Long.BYTES);
    private static final int VENUE_ID_OFFSET = (5 * Long.BYTES);
    private static final int SYMBOL_LENGTH_OFFSET = (5 * Long.BYTES) + Integer.BYTES;
    private static final int SYMBOL_CHAR_BASE_OFFSET = (5 * Long.BYTES) + (2 * Integer.BYTES);

    @Override
    public void store(final ByteBuffer buffer, final int offset, final Order value)
    {
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
    public void load(final ByteBuffer buffer, final int offset, final Order container)
    {
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
    }

    @Override
    public long getId(final Order value)
    {
        return value.getId();
    }
}
