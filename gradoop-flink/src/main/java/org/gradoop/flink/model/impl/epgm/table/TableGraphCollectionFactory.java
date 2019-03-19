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

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.layouts.table.TableGraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.table.TableGraphCollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.table.TableSet;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Responsible for creating instances of {@link TableGraphCollection} based on a specific
 * {@link TableGraphCollectionLayout}.
 */
public class TableGraphCollectionFactory implements
  BaseGraphCollectionFactory<GraphHead, Vertex, Edge, TableGraphCollection, TableLogicalGraph,
    TableGraphCollectionLayoutFactory> {

  /**
   * The Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates the layout from given data.
   */
  private TableGraphCollectionLayoutFactory layoutFactory;

  /**
   * Creates a new factory.
   *
   * @param config Gradoop Flink configuration
   */
  public TableGraphCollectionFactory(GradoopFlinkConfig config) {
    this.config = config;
  }

  /**
   * Returns schema of current layout
   *
   * @return schema of current layout
   */
  public TableSetSchema getSchema() {
    return layoutFactory.getSchema();
  }

  /**
   * Creates a collection from the given table set.
   * Important: Table set needs to match schema of current layout!
   * @param tableSet table set
   * @return graph collection
   */
  public TableGraphCollection fromTableSet(TableSet tableSet) {
    return new TableGraphCollection(layoutFactory.fromTableSet(tableSet), this.config);
  }

  @Override
  public void setLayoutFactory(TableGraphCollectionLayoutFactory layoutFactory) {
    Objects.requireNonNull(layoutFactory);
    this.layoutFactory = layoutFactory;
    this.layoutFactory.setGradoopFlinkConfig(config);
  }

  @Override
  public TableGraphCollection fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return new TableGraphCollection(layoutFactory.fromDataSets(graphHeads, vertices, edges),
      this.config);
  }

  @Override
  public TableGraphCollection fromDataSets(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices) {
    return new TableGraphCollection(layoutFactory.fromDataSets(graphHeads, vertices), this.config);
  }

  @Override
  public TableGraphCollection fromGraph(TableLogicalGraph logicalGraph) {
    return new TableGraphCollection(layoutFactory.fromGraph(logicalGraph), this.config);
  }

  @Override
  public TableGraphCollection fromGraphs(TableLogicalGraph... logicalGraphs) {
    return new TableGraphCollection(layoutFactory.fromGraphs(logicalGraphs), this.config);
  }

  @Override
  public TableGraphCollection createEmptyCollection() {
    return new TableGraphCollection(layoutFactory.createEmptyCollection(), this.config);
  }

  @Override
  public EPGMGraphHeadFactory<GraphHead> getGraphHeadFactory() {
    return config.getGraphHeadFactory();
  }

  @Override
  public EPGMVertexFactory<Vertex> getVertexFactory() {
    return config.getVertexFactory();
  }

  @Override
  public EPGMEdgeFactory<Edge> getEdgeFactory() {
    return config.getEdgeFactory();
  }

  //----------------------------------------------------------------------------
  // UNIMPLEMENTED
  //----------------------------------------------------------------------------

  @Override
  public TableGraphCollection fromIndexedDataSets(Map<String, DataSet<GraphHead>> graphHeads,
    Map<String, DataSet<Vertex>> vertices, Map<String, DataSet<Edge>> edges) {
    throw new NotImplementedException();
  }

  @Override
  public TableGraphCollection fromCollections(Collection<GraphHead> graphHeads,
    Collection<Vertex> vertices, Collection<Edge> edges) {
    throw new NotImplementedException();
  }

  @Override
  public TableGraphCollection fromTransactions(DataSet<GraphTransaction> transactions) {
    throw new NotImplementedException();
  }

  @Override
  public TableGraphCollection fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer) {
    throw new NotImplementedException();
  }

}
