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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public final class ConnectorMaterializedViewDefinition
{
    private final String originalSql;
    private final String schema;
    private final String table;
    private final List<SchemaTableName> baseTables;
    private final Optional<String> owner;
    // A 1-to-1 map from view column name to a tuple of base table name + base table column name
    @JsonIgnore
    private Optional<Map<String, Map<SchemaTableName, String>>> viewToBaseColumnMap;

    @JsonCreator
    public ConnectorMaterializedViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("baseTables") List<SchemaTableName> baseTables,
            @JsonProperty("owner") Optional<String> owner)
    {
        this(originalSql, schema, table, baseTables, owner, Optional.empty());
    }

    @JsonIgnore
    private ConnectorMaterializedViewDefinition(
            String originalSql,
            String schema,
            String table,
            List<SchemaTableName> baseTables,
            Optional<String> owner,
            Optional<Map<String, Map<SchemaTableName, String>>> viewToBaseColumnMap)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.baseTables = unmodifiableList(new ArrayList<>(requireNonNull(baseTables, "baseTables is null")));
        this.owner = requireNonNull(owner, "owner is null");
        this.viewToBaseColumnMap = requireNonNull(viewToBaseColumnMap, "viewToBaseColumnMap is null").map(columnMap -> unmodifiableMap(new HashMap<>(columnMap)));
    }

    @JsonProperty
    public String getOriginalSql()
    {
        return originalSql;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public List<SchemaTableName> getBaseTables()
    {
        return baseTables;
    }

    @JsonProperty
    public Optional<String> getOwner()
    {
        return owner;
    }

    @JsonIgnore
    public Optional<Map<String, Map<SchemaTableName, String>>> getViewToBaseColumnMap()
    {
        return viewToBaseColumnMap;
    }

    @JsonIgnore
    public ConnectorMaterializedViewDefinition withViewToBaseColumnMap(Map<String, Map<SchemaTableName, String>> viewToBaseColumnMap)
    {
        return new ConnectorMaterializedViewDefinition(this.originalSql, this.schema, this.table, this.baseTables, this.owner, Optional.of(viewToBaseColumnMap));
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorMaterializedViewDefinition{");
        sb.append("originalSql=").append(originalSql);
        sb.append(",schema=").append(schema);
        sb.append(",table=").append(table);
        sb.append(",baseTables=").append(baseTables);
        sb.append(",owner=").append(owner.orElse(null));
        sb.append("}");
        return sb.toString();
    }
}
