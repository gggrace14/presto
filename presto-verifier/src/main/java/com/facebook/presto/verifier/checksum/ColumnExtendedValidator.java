package com.facebook.presto.verifier.checksum;

import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.verifier.framework.Column;

import java.util.List;

public interface ColumnExtendedValidator
        extends ColumnValidator
{
    List<SingleColumn> generateExtendedChecksumColumns(Column column);

    List<? extends ColumnMatchResult<?>> validateExtended(Column column, ChecksumResult controlResult, ChecksumResult testResult);
}
