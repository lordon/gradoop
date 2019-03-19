/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
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
package org.gradoop.flink.model.impl.layouts.table.common.operators.grouping;

import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Base class for all builders of table based grouping operators
 */
public abstract class TableGroupingBuilderBase {

  /**
   * Used as property key to declare a label based grouping.*
   */
  public static final String LABEL_SYMBOL = ":label";

  /**
   * True, iff vertex labels shall be considered.
   */
  protected boolean useVertexLabel;

  /**
   * True, iff edge labels shall be considered.
   */
  protected boolean useEdgeLabel;

  /**
   * List of property keys to group vertices by
   */
  protected final List<String> vertexPropertyKeys;

  /**
   * List of aggregate functions to execute on grouped vertices
   */
  protected final List<AggregateFunction> vertexAggregateFunctions;

  /**
   * List of property keys to group edges by
   */
  protected final List<String> edgePropertyKeys;

  /**
   * List of aggregate functions to execute on grouped edges
   */
  protected final List<AggregateFunction> edgeAggregateFunctions;


  /**
   * Creates a new grouping builder
   */
  public TableGroupingBuilderBase() {
    this.useVertexLabel           = false;
    this.useEdgeLabel             = false;
    this.vertexPropertyKeys       = new ArrayList<>();
    this.vertexAggregateFunctions = new ArrayList<>();
    this.edgePropertyKeys         = new ArrayList<>();
    this.edgeAggregateFunctions   = new ArrayList<>();
  }

  /**
   * Adds a property key to the vertex grouping keys
   *
   * @param key property key
   * @return this builder
   */
  public TableGroupingBuilderBase addVertexGroupingKey(String key) {
    Objects.requireNonNull(key);
    if (key.equals(LABEL_SYMBOL)) {
      useVertexLabel(true);
    } else {
      vertexPropertyKeys.add(key);
    }
    return this;
  }

  /**
   * Adds a list of property keys to the vertex grouping keys
   *
   * @param keys property keys
   * @return this builder
   */
  public TableGroupingBuilderBase addVertexGroupingKeys(List<String> keys) {
    Objects.requireNonNull(keys);
    for (String key : keys) {
      this.addVertexGroupingKey(key);
    }
    return this;
  }

  /**
   * Adds a property key to the edge grouping keys
   *
   * @param key property key
   * @return this builder
   */
  public TableGroupingBuilderBase addEdgeGroupingKey(String key) {
    Objects.requireNonNull(key);
    if (key.equals(LABEL_SYMBOL)) {
      useEdgeLabel(true);
    } else {
      edgePropertyKeys.add(key);
    }
    return this;
  }

  /**
   * Adds a list of property keys to the edge grouping keys
   *
   * @param keys property keys
   * @return this builder
   */
  public TableGroupingBuilderBase addEdgeGroupingKeys(List<String> keys) {
    Objects.requireNonNull(keys);
    for (String key : keys) {
      this.addEdgeGroupingKey(key);
    }
    return this;
  }

  /**
   * Add an aggregate function which is applied on all vertices represented by a single super
   * vertex
   *
   * @param aggregateFunction vertex aggregate function
   * @return this builder
   */
  public TableGroupingBuilderBase addVertexAggregateFunction(AggregateFunction aggregateFunction) {
    Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
    vertexAggregateFunctions.add(aggregateFunction);
    return this;
  }

  /**
   * Add an aggregate function which is applied on all edges represented by a single super edge
   *
   * @param aggregateFunction edge aggregate function
   * @return this builder
   */
  public TableGroupingBuilderBase addEdgeAggregateFunction(AggregateFunction aggregateFunction) {
    Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
    edgeAggregateFunctions.add(aggregateFunction);
    return this;
  }


  /**
   * Define, if the vertex label shall be used for grouping vertices.
   *
   * @param useVertexLabel true, iff vertex label shall be used for grouping
   * @return this builder
   */
  public TableGroupingBuilderBase useVertexLabel(boolean useVertexLabel) {
    this.useVertexLabel = useVertexLabel;
    return this;
  }

  /**
   * Define, if the edge label shall be used for grouping edges.
   *
   * @param useEdgeLabel true, iff edge label shall be used for grouping
   * @return this builder
   */
  public TableGroupingBuilderBase useEdgeLabel(boolean useEdgeLabel) {
    this.useEdgeLabel = useEdgeLabel;
    return this;
  }

  /**
   * Build grouping operator instance
   *
   * @return grouping operator instance
   */
  public abstract TableGroupingBase build();

}
