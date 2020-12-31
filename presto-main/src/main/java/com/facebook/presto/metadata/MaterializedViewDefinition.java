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
package com.facebook.presto.metadata;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public final class MaterializedViewDefinition
{
    private final String originalSql;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final List<ViewColumn> columns;
    private final List<ViewBaseTable> baseTables;
    private final Optional<String> owner;
    @JsonIgnore
    private Optional<PartitionSpecs> partitionsFromView = Optional.empty();
    @JsonIgnore
    private Optional<Map<ViewBaseTable, PartitionSpecs>> partitionsFromBaseTables = Optional.empty();

    @JsonCreator
    public MaterializedViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("columns") List<ViewColumn> columns,
            @JsonProperty("baseTables") List<ViewBaseTable> baseTables,
            @JsonProperty("owner") Optional<String> owner)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.baseTables = ImmutableList.copyOf(requireNonNull(baseTables, "baseTables is null"));
        this.owner = requireNonNull(owner, "owner is null");
    }

    @JsonProperty
    public String getOriginalSql()
    {
        return originalSql;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<ViewColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<ViewBaseTable> getBaseTables()
    {
        return baseTables;
    }

    @JsonProperty
    public Optional<String> getOwner()
    {
        return owner;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("originalSql", originalSql)
                .add("catalog", catalog.orElse(null))
                .add("schema", schema.orElse(null))
                .add("columns", columns)
                .add("baseTables", baseTables)
                .add("owner", owner.orElse(null))
                .omitNullValues()
                .toString();
    }

    public MaterializedViewDefinition setViewPartitions(PartitionSpecs partitionSpecs)
    {
        this.partitionsFromView = Optional.of(requireNonNull(partitionSpecs, "partitionSpecs is null"));
        return this;
    }

    public MaterializedViewDefinition addBaseTablePartitions(ViewBaseTable baseTable, PartitionSpecs baseTablePartitionSpecs)
    {
        requireNonNull(baseTable, "baseTable is null.");
        requireNonNull(baseTablePartitionSpecs, "baseTablePartitionSpecs is null");

        if (!this.partitionsFromView.isPresent()) {
            return this;
        }

        if (!this.partitionsFromBaseTables.isPresent()) {
            this.partitionsFromBaseTables = Optional.of(new HashMap<ViewBaseTable, PartitionSpecs>());
        }
        this.partitionsFromBaseTables.get().put(baseTable, PartitionSpecs.difference(baseTablePartitionSpecs, this.partitionsFromView.get()));
        return this;
    }

    public boolean isFresh()
    {
        return Iterables.all(this.partitionsFromBaseTables.orElse(Collections.emptyMap()).values(), PartitionSpecs::isEmpty);
    }

    public static final class ViewBaseTable
    {
        private final SchemaTableName name;

        @JsonCreator
        public ViewBaseTable(
                @JsonProperty("name") SchemaTableName name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        @JsonProperty
        public SchemaTableName getName()
        {
            return name;
        }

        @Override
        public String toString()
        {
            return name.toString();
        }
    }

    public static final class ViewColumn
    {
        private final String name;
        private final Type type;

        @JsonCreator
        public ViewColumn(
                @JsonProperty("name") String name,
                @JsonProperty("type") Type type)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public Type getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return name + ":" + type;
        }
    }

    @JsonIgnoreType
    public static final class PartitionSpecs
    {
        private final List<Map<String, String>> partitions;

        public PartitionSpecs(List<Map<String, String>> partitionSpecs)
        {
            this.partitions = ImmutableList.copyOf(requireNonNull(partitionSpecs, "partitionSpecs is null"));
        }

        public static PartitionSpecs of(List<Map<String, String>> partitionSpecs)
        {
            return new PartitionSpecs(partitionSpecs);
        }

        public static PartitionSpecs empty()
        {
            return new PartitionSpecs(Collections.emptyList());
        }

        public static PartitionSpecs difference(PartitionSpecs left, PartitionSpecs right)
        {
            if (left.partitions.isEmpty() || right.partitions.isEmpty()) {
                return empty();
            }
            Map<String, String> leftFirst = left.partitions.iterator().next();
            Map<String, String> rightFirst = right.partitions.iterator().next();
            Sets.SetView commonKeys = Sets.intersection(leftFirst.keySet(), rightFirst.keySet());
            if (commonKeys.isEmpty()) {
                return empty();
            }
            Function<Map<String, String>, Map<String, String>> toSpecOnCommonKeys = partSpec -> Maps.filterKeys(partSpec, key -> commonKeys.contains(key));

            Set<Map<String, String>> rightOnCommonKeys = right.partitions
                    .stream()
                    .map(toSpecOnCommonKeys)
                    .collect(toImmutableSet());

            List<Map<String, String>> diff = new ArrayList<>();
            Iterator<Map<String, String>> leftIter = left.partitions.iterator();
            while (leftIter.hasNext()) {
                if (toSpecOnCommonKeys.apply(leftIter.next()).equals(toSpecOnCommonKeys.apply(rightFirst))) {
                    break;
                }
            }
            while (leftIter.hasNext()) {
                Map<String, String> leftSpec = leftIter.next();
                if (!rightOnCommonKeys.contains(toSpecOnCommonKeys.apply(leftSpec))) {
                    diff.add(leftSpec);
                }
            }
            return PartitionSpecs.of(diff);
        }

        public boolean isEmpty()
        {
            return partitions.isEmpty();
        }
    }
}
