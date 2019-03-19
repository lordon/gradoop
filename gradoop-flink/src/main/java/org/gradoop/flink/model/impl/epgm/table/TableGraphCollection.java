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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.table.TableDataSink;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.GraphCollectionBaseOperators;
import org.gradoop.flink.model.api.layouts.table.BaseTableSet;
import org.gradoop.flink.model.api.layouts.table.TableGraphCollectionLayout;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Objects;

/**
 * A graph collection graph is one of the base concepts of the Extended Property Graph Model.
 * From a model perspective, the collection represents a set of logical graphs.
 *
 * A table graph collection is an API wrapper for a graph collection implemented using Flink's
 * Table-API. In contrast to {@link GraphCollection} a table graph collection is based on Tables
 * instead of DataSets.
 *
 * A table graph collection is wrapping a {@link TableGraphCollectionLayout} which defines, how the
 * collection is represented in Apache Flink. A {@link TableGraphCollectionLayout} refers to a
 * set of tables, which represent the EPGM. Both, {@link TableGraphCollection} and
 * {@link TableGraphCollectionLayout} implement API methods {@link GraphCollectionBaseOperators}
 * while {@link TableGraphCollection just forward the calls to the layout. This is just for
 * convenience and API synchronicity.
 */
public class TableGraphCollection implements
  BaseGraphCollection<GraphHead, Vertex, Edge, TableGraphCollection,
    TableLogicalGraph, TableGraphCollectionFactory>,
  GraphCollectionBaseOperators<TableLogicalGraph, TableGraphCollection, TableDataSink> {

  /**
   * Layout for that graph collection
   */
  private final TableGraphCollectionLayout layout;

  /**
   * Configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param layout the graph collection layout
   * @param config the Gradoop Flink configuration
   */
  TableGraphCollection(TableGraphCollectionLayout layout, GradoopFlinkConfig config) {
    this.layout = Objects.requireNonNull(layout);
    this.config = Objects.requireNonNull(config);
  }

  /**
   * Get layout of current graph collection
   *
   * @return layout of current graph collection
   */
  public TableGraphCollectionLayout getLayout() {
    return layout;
  }

  /**
   * Get table set of current graph collection
   *
   * @return table set of current graph collection
   */
  public BaseTableSet getTableSet() {
    return layout.getTableSet();
  }

  @Override
  public GradoopFlinkConfig getConfig() {
    return this.config;
  }

  @Override
  public TableGraphCollectionFactory getFactory() {
    return this.config.getTableGraphCollectionFactory();
  }

  //----------------------------------------------------------------------------
  // Base operators as facade for table layout implementations
  //----------------------------------------------------------------------------

  @Override
  public TableLogicalGraph getGraph(GradoopId graphID) {
    return layout.getGraph(graphID);
  }

  @Override
  public TableGraphCollection getGraphs(GradoopId... identifiers) {
    return layout.getGraphs(identifiers);
  }

  @Override
  public TableGraphCollection getGraphs(GradoopIdSet identifiers) {
    return layout.getGraphs(identifiers);
  }

  @Override
  public DataSet<Boolean> isEmpty() {
    return layout.isEmpty();
  }

  @Override
  public TableGraphCollection union(TableGraphCollection otherCollection) {
    return this.layout.union(otherCollection);
  }

  @Override
  public TableGraphCollection intersect(TableGraphCollection otherCollection) {
    return this.layout.intersect(otherCollection);
  }

  @Override
  public TableGraphCollection intersectWithSmallResult(TableGraphCollection otherCollection) {
    return this.layout.intersectWithSmallResult(otherCollection);
  }

  @Override
  public TableGraphCollection difference(TableGraphCollection otherCollection) {
    return this.layout.difference(otherCollection);
  }

  @Override
  public TableGraphCollection differenceWithSmallResult(TableGraphCollection otherCollection) {
    return this.layout.differenceWithSmallResult(otherCollection);
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
  public boolean isTransactionalLayout() {
    return false;
  }

  @Override
  public DataSet<GraphHead> getGraphHeads() {
    return layout.toGraphHeadDataSet();
  }

  @Override
  public DataSet<GraphHead> getGraphHeadsByLabel(String label) {
    return layout.toGraphHeadDataSet().filter(new ByLabel<>(label));
  }

  @Override
  public DataSet<GraphTransaction> getGraphTransactions() {
    // Detour over GVE data set layout
    return (new GVECollectionLayoutFactory()).fromDataSets(getGraphHeads(), getVertices(),
      getEdges()).getGraphTransactions();
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
