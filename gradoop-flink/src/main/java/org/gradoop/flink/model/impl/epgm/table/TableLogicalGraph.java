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
package org.gradoop.flink.model.impl.epgm.table;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.table.TableDataSink;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.table.TableLogicalGraphOperators;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayout;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A logical graph is one of the base concepts of the Extended Property Graph Model.
 *
 * Furthermore, a logical graph provides operations that are performed on the underlying data. These
 * operations result in either another logical graph or in a {@link TableGraphCollection}.
 *
 * A table graph collection is an API wrapper for a logical graph implemented using Flink's
 * Table-API. In contrast to {@link LogicalGraph} a table graph collection is based on Tables
 * instead of DataSets.
 *
 * A logical graph is wrapping a {@link TableLogicalGraphLayout} which defines, how the graph is
 * represented in Apache Flink.  A {@link TableLogicalGraphLayout} refers to a set of tables,
 * which represent the EPGM. Both, {@link TableGraphCollection} and{@link TableLogicalGraphLayout}
 * implement API methods {@link TableLogicalGraphOperators} while {@link TableLogicalGraph just
 * forward the calls to the layout. This is just for
 * convenience and API synchronicity.
 */
public class TableLogicalGraph implements
  BaseGraph<GraphHead, Vertex, Edge, TableLogicalGraph, TableLogicalGraphFactory>,
  TableLogicalGraphOperators<TableLogicalGraph, TableGraphCollection, TableDataSink> {

  /**
   * Layout for that logical graph.
   */
  private final TableLogicalGraphLayout layout;

  /**
   * Configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new logical graph based on the given parameters.
   *
   * @param layout representation of the logical graph
   * @param config the Gradoop Flink configuration
   */
  TableLogicalGraph(TableLogicalGraphLayout layout, GradoopFlinkConfig config) {
    Objects.requireNonNull(layout);
    Objects.requireNonNull(config);
    this.layout = layout;
    this.config = config;
  }

  /**
   * Get layout of current graph collection
   *
   * @return layout of current graph collection
   */
  public TableLogicalGraphLayout getLayout() {
    return layout;
  }

  @Override
  public GradoopFlinkConfig getConfig() {
    return this.config;
  }

  @Override
  public TableLogicalGraphFactory getFactory() {
    return this.config.getTableLogicalGraphFactory();
  }

  //----------------------------------------------------------------------------
  // Base operators as facade for table layout implementations
  //----------------------------------------------------------------------------

  @Override
  public DataSet<Boolean> isEmpty() {
    return this.layout.isEmpty();
  }

  @Override
  public TableLogicalGraph combine(TableLogicalGraph otherGraph) {
    return this.layout.combine(otherGraph);
  }

  @Override
  public TableLogicalGraph overlap(TableLogicalGraph otherGraph) {
    return this.layout.overlap(otherGraph);
  }

  @Override
  public TableLogicalGraph exclude(TableLogicalGraph otherGraph) {
    return this.layout.exclude(otherGraph);
  }

  @Override
  public TableLogicalGraph vertexInducedSubgraph(String... vertexLabels) {
    return this.layout.vertexInducedSubgraph(vertexLabels);
  }

  @Override
  public TableLogicalGraph edgeInducedSubgraph(String... edgeLabels) {
    return this.layout.edgeInducedSubgraph(edgeLabels);
  }

  @Override
  public TableLogicalGraph subgraph(String[] vertexLabels, String[] edgeLabels,
    Subgraph.Strategy strategy) {
    return this.layout.subgraph(vertexLabels, edgeLabels, strategy);
  }

  @Override
  public TableLogicalGraph groupBy(List<String> vertexGroupingKeys) {
    return this.layout.groupBy(vertexGroupingKeys);
  }

  @Override
  public TableLogicalGraph groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    return this.layout.groupBy(vertexGroupingKeys, edgeGroupingKeys);
  }

  @Override
  public TableLogicalGraph groupBy(List<String> vertexGroupingKeys,
    List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions) {
    return this.layout.groupBy(vertexGroupingKeys, vertexAggregateFunctions, edgeGroupingKeys,
      edgeAggregateFunctions);
  }

  @Override
  public void writeTo(TableDataSink dataSink) throws IOException {
    this.layout.writeTo(dataSink);
  }

  @Override
  public void writeTo(TableDataSink dataSink, boolean overWrite) throws IOException {
    this.layout.writeTo(dataSink, overWrite);
  }

  //----------------------------------------------------------------------------
  // Simple data set compatibility
  //----------------------------------------------------------------------------

  @Override
  public boolean isGVELayout() {
    return false;
  }

  @Override
  public boolean isIndexedGVELayout() {
    return false;
  }

  @Override
  public DataSet<GraphHead> getGraphHead() {
    return layout.toGraphHeadDataSet();
  }

  @Override
  public DataSet<Vertex> getVertices() {
    return layout.toVertexDataSet();
  }

  @Override
  public DataSet<Vertex> getVerticesByLabel(String label) {
    return layout.toVertexDataSet().filter(new ByLabel<>(label));
  }

  @Override
  public DataSet<Edge> getEdges() {
    return layout.toEdgeDataSet();
  }

  @Override
  public DataSet<Edge> getEdgesByLabel(String label) {
    return layout.toEdgeDataSet().filter(new ByLabel<>(label));
  }
}
