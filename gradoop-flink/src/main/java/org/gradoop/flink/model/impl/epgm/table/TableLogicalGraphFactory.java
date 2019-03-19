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
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayout;
import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.layouts.table.TableSet;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Responsible for creating instances of {@link TableLogicalGraph} based on a specific
 * {@link TableLogicalGraphLayout}.
 */
public class TableLogicalGraphFactory implements
  BaseGraphFactory<GraphHead, Vertex, Edge, TableLogicalGraph, TableLogicalGraphLayoutFactory> {

  /**
   * The Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;
  /**
   * Creates the layout from given dataString tableNamePrefix, .
   */
  private TableLogicalGraphLayoutFactory layoutFactory;

  /**
   * Creates a new factory.
   *
   * @param config the Gradoop Flink configuration
   */
  public TableLogicalGraphFactory(GradoopFlinkConfig config) {
    this.config = config;
  }

  @Override
  public void setLayoutFactory(TableLogicalGraphLayoutFactory layoutFactory) {
    Objects.requireNonNull(layoutFactory);
    this.layoutFactory = layoutFactory;
    this.layoutFactory.setGradoopFlinkConfig(config);
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
   * Creates a logical graph from the given table set.
   * Important: Table set needs to match schema of current layout!
   * @param tableSet table set
   * @return logical graph
   */
  public TableLogicalGraph fromTableSet(TableSet tableSet) {
    return new TableLogicalGraph(layoutFactory.fromTableSet(tableSet),
      this.config);
  }

  @Override
  public TableLogicalGraph fromDataSets(DataSet<GraphHead> graphHead, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return new TableLogicalGraph(layoutFactory.fromDataSets(graphHead, vertices, edges),
      this.config);
  }

  @Override
  public TableLogicalGraph fromDataSets(DataSet<Vertex> vertices) {
    return new TableLogicalGraph(layoutFactory.fromDataSets(vertices), this.config);
  }

  @Override
  public TableLogicalGraph fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges) {
    return new TableLogicalGraph(layoutFactory.fromDataSets(vertices, edges), this.config);
  }

  @Override
  public TableLogicalGraph createEmptyGraph() {
    return new TableLogicalGraph(layoutFactory.createEmptyGraph(), this.config);
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
  public TableLogicalGraph fromIndexedDataSets(Map<String, DataSet<Vertex>> vertices,
    Map<String, DataSet<Edge>> edges) {
    return null;
  }

  @Override
  public TableLogicalGraph fromIndexedDataSets(Map<String, DataSet<GraphHead>> graphHead,
    Map<String, DataSet<Vertex>> vertices, Map<String, DataSet<Edge>> edges) {
    return null;
  }

  @Override
  public TableLogicalGraph fromCollections(GraphHead graphHead, Collection<Vertex> vertices,
    Collection<Edge> edges) {
    return null;
  }

  @Override
  public TableLogicalGraph fromCollections(Collection<Vertex> vertices, Collection<Edge> edges) {
    return null;
  }

}
