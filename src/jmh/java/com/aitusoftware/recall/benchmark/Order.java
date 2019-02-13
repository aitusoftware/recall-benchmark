package com.aitusoftware.recall.benchmark;

import com.aitusoftware.recall.persistence.AsciiCharSequence;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;

public final class Order implements BytesMarshallable
{
    private long id;
    private double quantity;
    private double price;
    private long sessionId;
    private int venueId;
    private long timestamp;
    private AsciiCharSequence symbol = new AsciiCharSequence(64);

    public void set(
        final long id, final double quantity, final double price,
        final long sessionId, final int venueId, final long timestamp, final String symbol)
    {
        this.id = id;
        this.quantity = quantity;
        this.price = price;
        this.sessionId = sessionId;
        this.venueId = venueId;
        this.timestamp = timestamp;
        this.symbol = new AsciiCharSequence(symbol);
    }

    @Override
    public void readMarshallable(final BytesIn buffer) throws IORuntimeException
    {
        setId(buffer.readLong());
        setSessionId(buffer.readLong());
        setTimestamp(buffer.readLong());
        setQuantity(Double.longBitsToDouble(buffer.readLong()));
        setPrice(Double.longBitsToDouble(buffer.readLong()));
        setVenueId(buffer.readInt());
        final int symbolLength = buffer.readInt();
        final AsciiCharSequence symbolSequence = getSymbolSequence();
        symbolSequence.reset();
        for (int i = 0; i < symbolLength; i++)
        {
            symbolSequence.append((char) buffer.readInt());
        }
    }

    @Override
    public void writeMarshallable(final BytesOut buffer)
    {
        buffer.writeLong(getId());
        buffer.writeLong(getSessionId());
        buffer.writeLong(getTimestamp());
        buffer.writeLong(Double.doubleToRawLongBits(getQuantity()));
        buffer.writeLong(Double.doubleToRawLongBits(getPrice()));
        buffer.writeInt(getVenueId());
        final int length = getSymbol().length();
        buffer.writeInt(length);
        for (int i = 0; i < length; i++)
        {
            buffer.writeInt(getSymbol().charAt(i));
        }
    }

    public long getId()
    {
        return id;
    }

    public void setId(final long id)
    {
        this.id = id;
    }

    public double getQuantity()
    {
        return quantity;
    }

    public void setQuantity(final double quantity)
    {
        this.quantity = quantity;
    }

    public double getPrice()
    {
        return price;
    }

    public void setPrice(final double price)
    {
        this.price = price;
    }

    public long getSessionId()
    {
        return sessionId;
    }

    public void setSessionId(final long sessionId)
    {
        this.sessionId = sessionId;
    }

    public int getVenueId()
    {
        return venueId;
    }

    public void setVenueId(final int venueId)
    {
        this.venueId = venueId;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(final long timestamp)
    {
        this.timestamp = timestamp;
    }

    public CharSequence getSymbol()
    {
        return symbol;
    }

    public AsciiCharSequence getSymbolSequence()
    {
        return symbol;
    }
}
