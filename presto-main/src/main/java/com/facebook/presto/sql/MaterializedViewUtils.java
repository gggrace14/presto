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

package com.facebook.presto.sql;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.MaterializedViewColumnMappingExtractor;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.ConnectorMaterializedViewDefinition.TableColumn;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class MaterializedViewUtils
{
    private MaterializedViewUtils() {}

    /**
     * Compute the 1-to-N column mapping from a materialized view to its base tables.
     * <p>
     * From {@code analysis}, we could derive only one base table column that one materialized view column maps to.
     * In case of 1 materialized view defined on N base tables via join, union, etc, this method helps compute
     * all the N base table columns that one materialized view column maps to.
     * It calls on MaterializedViewPlanValidator to get all base table columns linked by join, union, etc,
     * and then uses them to expand the 1-to-1 column mapping derived from {@code analysis} to the 1-to-N column mapping.
     * <p>
     * For example, given SELECT column_a AS column_x FROM table_a JOIN table_b ON (table_a.column_a = table_b.column_b),
     * the 1-to-1 column mapping from {@code analysis} is column_x -> table_a.column_a. Linked base table columns are
     * [table_a.column_a, table_b.column_b]. Then it will return a 1-to-N column mapping column_x -> {table_a.column_a, table_b.column_b}.
     */
    public static Map<String, Map<SchemaTableName, String>> computeMaterializedViewToBaseTableColumnMappings(Query viewQuery, Analysis analysis)
    {
        ImmutableMap.Builder<String, Map<SchemaTableName, String>> fullColumnMapping = ImmutableMap.builder();

        Map<String, Map<SchemaTableName, String>> originalColumnMapping = getOriginalColumnMappingFromAnalysis(viewQuery, analysis);

        List<List<TableColumn>> mappedBaseColumns = MaterializedViewColumnMappingExtractor.extractMappedBaseColumns(viewQuery, analysis);

        for (Map.Entry<String, Map<SchemaTableName, String>> entry : originalColumnMapping.entrySet()) {
            String viewColumn = entry.getKey();
            Map<SchemaTableName, String> originalBaseColumns = entry.getValue();

            Map<SchemaTableName, String> fullBaseColumns = new HashMap<SchemaTableName, String>() {{
                    putAll(originalBaseColumns); }};

            originalBaseColumns.forEach((originalBaseTable, originalBaseColumn) -> {
                TableColumn originalTableColumn = new TableColumn(originalBaseTable, originalBaseColumn);
                mappedBaseColumns.forEach(linkedTableColumnPair -> {
                    if (originalTableColumn.equals(linkedTableColumnPair.get(0))) {
                        fullBaseColumns.put(linkedTableColumnPair.get(1).getTableName(), linkedTableColumnPair.get(1).getColumnName());
                    }
                    else if (originalTableColumn.equals(linkedTableColumnPair.get(1))) {
                        fullBaseColumns.put(linkedTableColumnPair.get(0).getTableName(), linkedTableColumnPair.get(0).getColumnName());
                    }
                });
            });

            fullColumnMapping.put(viewColumn, ImmutableMap.copyOf(fullBaseColumns));
        }

        return fullColumnMapping.build();
    }

    private static Map<String, Map<SchemaTableName, String>> getOriginalColumnMappingFromAnalysis(Node viewQuery, Analysis analysis)
    {
        return analysis.getOutputDescriptor(viewQuery).getVisibleFields().stream()
                .filter(field -> field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent())
                .collect(toImmutableMap(
                        field -> field.getName().get(),
                        field -> ImmutableMap.of(toSchemaTableName(field.getOriginTable().get()), field.getOriginColumnName().get())));
    }
}
