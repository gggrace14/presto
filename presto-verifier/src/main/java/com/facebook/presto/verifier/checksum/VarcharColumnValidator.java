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
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.QueryUtil.functionCall;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static java.util.Objects.requireNonNull;

public class VarcharColumnValidator
        implements ColumnValidator
{
    private final FloatingPointColumnValidator floatingPointValidator;
    private final SimpleColumnValidator simpleColumnValidator;
    private final boolean validateStringAsDouble;

    @Inject
    public VarcharColumnValidator(VerifierConfig config, SimpleColumnValidator simpleColumnValidator, FloatingPointColumnValidator floatingPointValidator)
    {
        this.floatingPointValidator = requireNonNull(floatingPointValidator, "floatingPointValidator is null");
        this.simpleColumnValidator = requireNonNull(simpleColumnValidator, "simpleColumnValidator is null");
        this.validateStringAsDouble = config.isValidateStringAsDouble();
    }

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        ImmutableList.Builder<SingleColumn> columnsBuilder = ImmutableList.builder();

        columnsBuilder.addAll(simpleColumnValidator.generateChecksumColumns(column));

        if (validateStringAsDouble) {
            Expression nullCount = functionCall("count_if", new IsNullPredicate(column.getExpression()));
            Expression asDoubleNullCount = functionCall("count_if", new IsNullPredicate(getAsDoubleExpression(column)));

            Column asDoubleColumn = getAsDoubleColumn(column);
            columnsBuilder.addAll(floatingPointValidator.generateChecksumColumns(asDoubleColumn));
            columnsBuilder.add(new SingleColumn(nullCount, delimitedIdentifier(ColumnValidatorUtil.getNullCountColumnAlias(column))));
            columnsBuilder.add(new SingleColumn(asDoubleNullCount, delimitedIdentifier(ColumnValidatorUtil.getAsDoubleNullCountColumnAlias(column))));
        }

        return columnsBuilder.build();
    }

    @Override
    public List<ColumnMatchResult<VarcharColumnChecksum>> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        ColumnMatchResult<SimpleColumnChecksum> simpleMatchResult = simpleColumnValidator.validate(column, controlResult, testResult).get(0);

        VarcharColumnChecksum controlChecksum = toColumnChecksum(simpleMatchResult.getControlChecksum(), Optional.empty());
        VarcharColumnChecksum testChecksum = toColumnChecksum(simpleMatchResult.getTestChecksum(), Optional.empty());

        if (!validateStringAsDouble || !ColumnValidatorUtil.isDoubleAsStringColumn(column, controlResult, testResult)) {
            return ImmutableList.of(new ColumnMatchResult<>(simpleMatchResult.isMatched(), column, controlChecksum, testChecksum));
        }

        ColumnMatchResult<FloatingPointColumnChecksum> toDoubleMatchResult = floatingPointValidator.validate(getAsDoubleColumn(column), controlResult, testResult).get(0);
        controlChecksum = toColumnChecksum(simpleMatchResult.getControlChecksum(), Optional.of(toDoubleMatchResult.getControlChecksum()));
        testChecksum = toColumnChecksum(simpleMatchResult.getTestChecksum(), Optional.of(toDoubleMatchResult.getTestChecksum()));

        return ImmutableList.of(new ColumnMatchResult<>(toDoubleMatchResult.isMatched(), column, controlChecksum, testChecksum));
    }

    private static VarcharColumnChecksum toColumnChecksum(SimpleColumnChecksum simpleChecksum, Optional<FloatingPointColumnChecksum> asDoubleChecksum)
    {
        return new VarcharColumnChecksum(simpleChecksum.getChecksum(), asDoubleChecksum);
    }

    private static Column getAsDoubleColumn(Column column)
    {
        return Column.create(column.getName() + "_as_double", getAsDoubleExpression(column), DOUBLE);
    }

    private static Expression getAsDoubleExpression(Column column)
    {
        return new Cast(column.getExpression(), DOUBLE.getDisplayName(), true, false);
    }
}
