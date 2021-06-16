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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SetOperation;
import com.google.common.collect.ImmutableList;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.ConnectorMaterializedViewDefinition.TableColumn;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

// TODO: Add more cases https://github.com/prestodb/presto/issues/16032
public class MaterializedViewPlanValidator
        extends DefaultTraversalVisitor<Void, MaterializedViewPlanValidator.MaterializedViewPlanValidatorContext>
{
    private final Node query;

    private final boolean isValidationOnly;
    private final Optional<Analysis> analysis;

    private ImmutableList.Builder<List<TableColumn>> linkedBaseColumns;

    public MaterializedViewPlanValidator(Node query)
    {
        this(query, true, Optional.empty());
    }

    public MaterializedViewPlanValidator(Node query, boolean isValidationOnly, Optional<Analysis> analysis)
    {
        checkArgument(isValidationOnly || analysis.isPresent(), "analysis must be set if");

        this.query = requireNonNull(query, "query is null");
        this.isValidationOnly = isValidationOnly;
        this.analysis = requireNonNull(analysis, "analysis is null");

        this.linkedBaseColumns = ImmutableList.builder();
    }

    public List<List<TableColumn>> getLinkedBaseColumns()
    {
        return linkedBaseColumns.build();
    }

    @Override
    protected Void visitJoin(Join node, MaterializedViewPlanValidatorContext context)
    {
        context.pushJoinNode(node);
        if (context.getJoinNodes().size() > 1) {
            throw new SemanticException(NOT_SUPPORTED, query, "More than one join in materialized view is not supported yet.");
        }

        if (!node.getType().equals(Join.Type.INNER)) {
            throw new SemanticException(NOT_SUPPORTED, node, "Only inner join is supported for materialized view.");
        }
        if (!node.getCriteria().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Join with no criteria is not supported for materialized view.");
        }

        JoinCriteria joinCriteria = node.getCriteria().get();
        if (!(joinCriteria instanceof JoinOn)) {
            throw new SemanticException(NOT_SUPPORTED, node, "Only join-on is supported for materialized view.");
        }

        process(node.getLeft(), context);
        process(node.getRight(), context);

        context.setProcessingJoinNode(true);
        process(((JoinOn) joinCriteria).getExpression(), context);
        context.setProcessingJoinNode(false);

        context.popJoinNode();
        return null;
    }

    @Override
    protected Void visitSetOperation(SetOperation node, MaterializedViewPlanValidatorContext context)
    {
        if (isValidationOnly) {
            return super.visitSetOperation(node, context);
        }

        List<RelationType> outputDescriptorList = node.getRelations().stream()
                .map(relation -> analysis.get().getOutputDescriptor(relation).withOnlyVisibleFields())
                .collect(toImmutableList());

        int numRelations = outputDescriptorList.size();
        int numFields = outputDescriptorList.get(0).getVisibleFieldCount();
        for (int f = 0; f < numFields; f++) {
            for (int r = 0; r < numRelations; r++) {
                Optional<TableColumn> firstBaseColumn = tryGetOriginalTableColumn(outputDescriptorList.get(r).getFieldByIndex(f));
                for (int t = 0; t < numRelations; t++) {
                    Optional<TableColumn> secondBaseColumn = tryGetOriginalTableColumn(outputDescriptorList.get(t).getFieldByIndex(f));
                    if (firstBaseColumn.isPresent() && secondBaseColumn.isPresent() && !firstBaseColumn.get().equals(secondBaseColumn.get())) {
                        linkedBaseColumns.add(ImmutableList.of(firstBaseColumn.get(), secondBaseColumn.get()));
                    }
                }
            }
        }

        return super.visitSetOperation(node, context);
    }

    @Override
    protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, MaterializedViewPlanValidatorContext context)
    {
        if (!context.isProcessingJoinNode()) {
            return super.visitLogicalBinaryExpression(node, context);
        }

        // TODO: It should only support equi join case https://github.com/prestodb/presto/issues/16033
        if (!node.getOperator().equals(LogicalBinaryExpression.Operator.AND)) {
            throw new SemanticException(NOT_SUPPORTED, node, "Only AND operator is supported for join criteria for materialized view.");
        }
        return super.visitLogicalBinaryExpression(node, context);
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, MaterializedViewPlanValidatorContext context)
    {
        if (!context.isProcessingJoinNode()) {
            return super.visitComparisonExpression(node, context);
        }

        if (!node.getOperator().equals(ComparisonExpression.Operator.EQUAL)) {
            throw new SemanticException(NOT_SUPPORTED, node, "Only EQUAL join is supported for materialized view.");
        }

        if (isValidationOnly) {
            return super.visitComparisonExpression(node, context);
        }

        Field left = analysis.get().getScope(context.getTopJoinNode()).tryResolveField(node.getLeft())
                .orElseThrow(() -> new SemanticException(
                        NOT_SUPPORTED,
                        node.getLeft(),
                        "%s in join criteria is not supported for materialized view.", node.getLeft().getClass().getSimpleName()))
                .getField();
        Field right = analysis.get().getScope(context.getTopJoinNode()).tryResolveField(node.getRight())
                .orElseThrow(() -> new SemanticException(
                        NOT_SUPPORTED,
                        node.getRight(),
                        "%s in join criteria is not supported for materialized view.", node.getRight().getClass().getSimpleName()))
                .getField();

        Optional<TableColumn> leftBaseColumn = tryGetOriginalTableColumn(left);
        Optional<TableColumn> rightBaseColumn = tryGetOriginalTableColumn(right);
        if (leftBaseColumn.isPresent() && rightBaseColumn.isPresent() && !leftBaseColumn.get().equals(rightBaseColumn.get())) {
            linkedBaseColumns.add(ImmutableList.of(leftBaseColumn.get(), rightBaseColumn.get()));
        }

        return super.visitComparisonExpression(node, context);
    }

    private Optional<TableColumn> tryGetOriginalTableColumn(Field field)
    {
        if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
            SchemaTableName table = toSchemaTableName(field.getOriginTable().get());
            String column = field.getOriginColumnName().get();
            return Optional.of(new TableColumn(table, column));
        }
        return Optional.empty();
    }

    public static final class MaterializedViewPlanValidatorContext
    {
        private boolean isProcessingJoinNode;
        private final LinkedList<Join> joinNodeStack;

        public MaterializedViewPlanValidatorContext()
        {
            isProcessingJoinNode = false;
            joinNodeStack = new LinkedList<>();
        }

        public boolean isProcessingJoinNode()
        {
            return isProcessingJoinNode;
        }

        public void setProcessingJoinNode(boolean processingJoinNode)
        {
            isProcessingJoinNode = processingJoinNode;
        }

        public void pushJoinNode(Join join)
        {
            joinNodeStack.push(join);
        }

        public Join popJoinNode()
        {
            return joinNodeStack.pop();
        }

        public Join getTopJoinNode()
        {
            return joinNodeStack.getFirst();
        }

        public List<Join> getJoinNodes()
        {
            return ImmutableList.copyOf(joinNodeStack);
        }
    }
}
