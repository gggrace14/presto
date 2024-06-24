/*
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
package com.facebook.presto.verifier.checksum;

import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.verifier.framework.Column;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static java.util.Objects.requireNonNull;

public class VarcharColumnValidator
        implements ColumnValidator
{
    private final FloatingPointColumnValidator floatingPointValidator;
    private final SimpleColumnValidator simpleColumnValidator;

    @Inject
    public VarcharColumnValidator(SimpleColumnValidator simpleColumnValidator, FloatingPointColumnValidator floatingPointValidator)
    {
        this.floatingPointValidator = requireNonNull(floatingPointValidator, "floatingPointValidator is null");
        this.simpleColumnValidator = requireNonNull(simpleColumnValidator, "simpleColumnValidator is null");
    }

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        ImmutableList.Builder<SingleColumn> columnsBuilder = ImmutableList.builder();

        columnsBuilder.addAll(simpleColumnValidator.generateChecksumColumns(column));
        columnsBuilder.add(new SingleColumn(
                new FunctionCall(
                        QualifiedName.of("count"),
                        Optional.empty(),
                        Optional.of(new IsNullPredicate(column.getExpression())),
                        Optional.empty(),
                        false,
                        ImmutableList.of(column.getExpression())),
                Optional.of(delimitedIdentifier(getNullCountColumnAlias(column)))));

        Expression toDoubleExpression = getToDoubleExpression(column);
        Column toDoubleColumn = getToDoubleColumn(column);

        columnsBuilder.addAll(floatingPointValidator.generateChecksumColumns(toDoubleColumn));
        columnsBuilder.add(new SingleColumn(
                new FunctionCall(
                        QualifiedName.of("count"),
                        Optional.empty(),
                        Optional.of(new IsNullPredicate(toDoubleExpression)),
                        Optional.empty(),
                        false,
                        ImmutableList.of(toDoubleExpression)),
                Optional.of(delimitedIdentifier(getNullDoubleCountColumnAlias(column)))));

        return columnsBuilder.build();
    }

    @Override
    public List<ColumnMatchResult<VarcharColumnChecksum>> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        ColumnMatchResult<SimpleColumnChecksum> simpleMatchResult = simpleColumnValidator.validate(column, controlResult, testResult).get(0);
        ColumnMatchResult<FloatingPointColumnChecksum> toDoubleMatchResult = floatingPointValidator.validate(getToDoubleColumn(column), controlResult, testResult).get(0);

        VarcharColumnChecksum controlChecksum = toColumnChecksum(column, simpleMatchResult.getControlChecksum(), toDoubleMatchResult.getControlChecksum(), controlResult);
        VarcharColumnChecksum testChecksum = toColumnChecksum(column, simpleMatchResult.getTestChecksum(), toDoubleMatchResult.getTestChecksum(), testResult);

        if (simpleMatchResult.isMatched()) {
            return ImmutableList.of(new ColumnMatchResult<>(true, column, controlChecksum, testChecksum));
        }
        if (controlChecksum.getNullCount() != controlChecksum.getNullDoubleCount() || testChecksum.getNullCount() != testChecksum.getNullDoubleCount()) {
            return ImmutableList.of(new ColumnMatchResult<>(false, column, controlChecksum, testChecksum));
        }
        return ImmutableList.of(new ColumnMatchResult<>(toDoubleMatchResult.isMatched(), column, controlChecksum, testChecksum));
    }

    private static VarcharColumnChecksum toColumnChecksum(Column column, SimpleColumnChecksum simpleChecksum, FloatingPointColumnChecksum toDoubleChecksum,
            ChecksumResult allChecksumResult)
    {
        if (allChecksumResult.getRowCount() == 0) {
            return new VarcharColumnChecksum(null, null, 0, 0, 0, 0, 0, 0);
        }
        return new VarcharColumnChecksum(
                simpleChecksum.getChecksum(),
                toDoubleChecksum.getSum(),
                toDoubleChecksum.getNanCount(),
                toDoubleChecksum.getPositiveInfinityCount(),
                toDoubleChecksum.getNegativeInfinityCount(),
                (long) allChecksumResult.getChecksum(getNullCountColumnAlias(column)),
                (long) allChecksumResult.getChecksum(getNullDoubleCountColumnAlias(column)),
                allChecksumResult.getRowCount());
    }

    private static Column getToDoubleColumn(Column column)
    {
        return Column.create(column.getName() + "_to_double", getToDoubleExpression(column), DOUBLE);
    }

    private static Expression getToDoubleExpression(Column column)
    {
        return new Cast(column.getExpression(), DOUBLE.getDisplayName(), true, false);
    }

    private static String getNullCountColumnAlias(Column column)
    {
        return column.getName() + "_to_double$null_count";
    }

    private static String getNullDoubleCountColumnAlias(Column column)
    {
        return column.getName() + "_to_double$null_count";
    }
}
