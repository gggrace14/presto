package com.facebook.presto.verifier.checksum;

import javax.annotation.Nullable;

import java.util.Objects;

import static java.lang.String.format;

public class VarcharColumnChecksum
        extends ColumnChecksum
{
    private final Object checksum;
    private final Object sum;
    private final long nanCount;
    private final long positiveInfinityCount;
    private final long negativeInfinityCount;
    private final long nullCount;
    private final long nullDoubleCount;
    private final long rowCount;

    public VarcharColumnChecksum(@Nullable Object checksum, @Nullable Object sum, long nanCount, long positiveInfinityCount, long negativeInfinityCount, long nullCount, long nullDoubleCount, long rowCount)
    {
        this.checksum = checksum;
        this.sum = sum;
        this.positiveInfinityCount = positiveInfinityCount;
        this.negativeInfinityCount = negativeInfinityCount;
        this.nanCount = nanCount;
        this.nullCount = nullCount;
        this.nullDoubleCount = nullDoubleCount;
        this.rowCount = rowCount;
    }

    @Nullable
    public Object getChecksum()
    {
        return checksum;
    }

    @Nullable
    public Object getSum()
    {
        return sum;
    }

    public long getNanCount()
    {
        return nanCount;
    }

    public long getPositiveInfinityCount()
    {
        return positiveInfinityCount;
    }

    public long getNegativeInfinityCount()
    {
        return negativeInfinityCount;
    }

    public long getNullCount()
    {
        return nullCount;
    }

    public long getNullDoubleCount()
    {
        return nullDoubleCount;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        VarcharColumnChecksum o = (VarcharColumnChecksum) obj;
        return Objects.equals(checksum, o.checksum) &&
                Objects.equals(sum, o.sum) &&
                Objects.equals(nanCount, o.nanCount) &&
                Objects.equals(positiveInfinityCount, o.positiveInfinityCount) &&
                Objects.equals(negativeInfinityCount, o.negativeInfinityCount) &&
                Objects.equals(nullCount, o.nullCount) &&
                Objects.equals(nullDoubleCount, o.nullDoubleCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(checksum, sum, nanCount, positiveInfinityCount, negativeInfinityCount, nullCount, nullDoubleCount);
    }

    @Override
    public String toString()
    {
        String mean = (rowCount > 0 && sum != null) ? ", mean: " + ((double) sum / rowCount) : "";
        return format("sum: %s, NaN: %s, +infinity: %s, -infinity: %s%s", sum, nanCount, positiveInfinityCount, negativeInfinityCount, mean);
    }
}
