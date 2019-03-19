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
package org.gradoop.flink.util;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.api.layouts.table.TableGraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.LogicalGraphFactory;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollectionFactory;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraphFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVEGraphLayoutFactory;
import org.gradoop.flink.model.impl.layouts.table.gve.TableGVEGraphCollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.table.gve.TableGVELogicalGraphLayoutFactory;

import java.util.Objects;

/**
 * Configuration for Gradoop running on Flink.
 */
public class GradoopFlinkConfig extends GradoopConfig<GraphHead, Vertex, Edge> {

  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment executionEnvironment;

  /**
   * Flink table execution environment.
   */
  private final BatchTableEnvironment tableEnvironment;

  /**
   * Creates instances of {@link LogicalGraph}
   */
  private final LogicalGraphFactory logicalGraphFactory;

  /**
   * Creates instances of {@link GraphCollection}
   */
  private final GraphCollectionFactory graphCollectionFactory;

  /**
   * Creates instances of {@link TableLogicalGraph}
   */
  private final TableLogicalGraphFactory tableLogicalGraphFactory;

  /**
   * Creates instances of {@link TableGraphCollection}
   */
  private final TableGraphCollectionFactory tableGraphCollectionFactory;

  /**
   * Creates a new Configuration.
   *
   * @param executionEnvironment Flink execution environment
   * @param logicalGraphLayoutFactory Factory for creating logical graphs
   * @param graphCollectionLayoutFactory Factory for creating graph collections
   * @param tableLogicalGraphLayoutFactory Factory for creating logical graphs in table layout
   * @param tableGraphCollectionLayoutFactory Factory for creating graph collections in table layout
   */
  protected GradoopFlinkConfig(
    ExecutionEnvironment executionEnvironment,
    LogicalGraphLayoutFactory<GraphHead, Vertex, Edge> logicalGraphLayoutFactory,
    GraphCollectionLayoutFactory<GraphHead, Vertex, Edge> graphCollectionLayoutFactory,
    TableLogicalGraphLayoutFactory tableLogicalGraphLayoutFactory,
    TableGraphCollectionLayoutFactory tableGraphCollectionLayoutFactory) {
    super();

    Objects.requireNonNull(executionEnvironment);
    Objects.requireNonNull(logicalGraphLayoutFactory);
    Objects.requireNonNull(graphCollectionLayoutFactory);

    this.executionEnvironment = executionEnvironment;
    this.tableEnvironment = TableEnvironment.getTableEnvironment(this.executionEnvironment);

    // init with default layout factories
    this.logicalGraphFactory = new LogicalGraphFactory(this);
    this.logicalGraphFactory.setLayoutFactory(logicalGraphLayoutFactory);

    this.graphCollectionFactory = new GraphCollectionFactory(this);
    this.graphCollectionFactory.setLayoutFactory(graphCollectionLayoutFactory);

    this.tableLogicalGraphFactory = new TableLogicalGraphFactory(this);
    this.tableLogicalGraphFactory.setLayoutFactory(tableLogicalGraphLayoutFactory);

    this.tableGraphCollectionFactory = new TableGraphCollectionFactory(this);
    this.tableGraphCollectionFactory.setLayoutFactory(tableGraphCollectionLayoutFactory);
  }

  /**
   * Creates a default Gradoop Flink configuration using POJO handlers.
   *
   * @param env Flink execution environment.
   *
   * @return the Gradoop Flink configuration
   */
  public static GradoopFlinkConfig createConfig(ExecutionEnvironment env) {
    return new GradoopFlinkConfig(env,
      new GVEGraphLayoutFactory(), new GVECollectionLayoutFactory(),
      new TableGVELogicalGraphLayoutFactory(), new TableGVEGraphCollectionLayoutFactory());
  }

  /**
   * Creates a Gradoop Flink configuration using the given parameters.
   *
   * @param env Flink execution environment
   * @param logicalGraphLayoutFactory factory to create logical graph layouts
   * @param graphCollectionLayoutFactory factory to create graph collection layouts
   * @return Gradoop Flink configuration
   */
  public static GradoopFlinkConfig createConfig(ExecutionEnvironment env,
    LogicalGraphLayoutFactory<GraphHead, Vertex, Edge> logicalGraphLayoutFactory,
    GraphCollectionLayoutFactory<GraphHead, Vertex, Edge> graphCollectionLayoutFactory) {
    return new GradoopFlinkConfig(env, logicalGraphLayoutFactory, graphCollectionLayoutFactory,
      new TableGVELogicalGraphLayoutFactory(), new TableGVEGraphCollectionLayoutFactory());
  }

  /**
   * Creates a Gradoop Flink configuration using the given parameters.
   *
   * @param env Flink execution environment
   * @param logicalGraphLayoutFactory factory to create logical graph layouts
   * @param graphCollectionLayoutFactory factory to create graph collection layouts
   * @param tableLogicalGraphLayoutFactory factory to create table based logical graph layouts
   * @param tableGraphCollectionLayoutFactory factory to create table graph collection layouts
   * @return Gradoop Flink configuration
   */
  public static GradoopFlinkConfig createConfig(ExecutionEnvironment env,
    LogicalGraphLayoutFactory<GraphHead, Vertex, Edge> logicalGraphLayoutFactory,
    GraphCollectionLayoutFactory<GraphHead, Vertex, Edge> graphCollectionLayoutFactory,
    TableLogicalGraphLayoutFactory tableLogicalGraphLayoutFactory,
    TableGraphCollectionLayoutFactory tableGraphCollectionLayoutFactory) {
    return new GradoopFlinkConfig(env, logicalGraphLayoutFactory, graphCollectionLayoutFactory,
      tableLogicalGraphLayoutFactory, tableGraphCollectionLayoutFactory);
  }

  /**
   * Returns the Flink execution environment.
   *
   * @return Flink execution environment
   */
  public ExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }

  /**
   * Returns the Flink table environment.
   *
   * @return Flink table environment
   */
  public BatchTableEnvironment getTableEnvironment() {
    return tableEnvironment;
  }

  /**
   * Returns a factory that is able to create logical graph layouts.
   *
   * @return factory for logical graph layouts
   */
  public LogicalGraphFactory getLogicalGraphFactory() {
    return logicalGraphFactory;
  }

  /**
   * Returns a factory that is able to create graph collection layouts.
   *
   * @return factory for graph collection layouts
   */
  public GraphCollectionFactory getGraphCollectionFactory() {
    return graphCollectionFactory;
  }

  /**
   * Returns a factory that is able to create logical graph layouts.
   *
   * @return factory for table logical graph layouts
   */
  public TableLogicalGraphFactory getTableLogicalGraphFactory() {
    return tableLogicalGraphFactory;
  }

  /**
   * Returns a factory that is able to create graph collection layouts.
   *
   * @return factory for graph collection layouts
   */
  public TableGraphCollectionFactory getTableGraphCollectionFactory() {
    return tableGraphCollectionFactory;
  }

  /**
   * Sets the layout factory for building layouts that represent a
   * {@link LogicalGraph}.
   *
   * @param factory logical graph layout factor
   */
  public void setLogicalGraphLayoutFactory(
    LogicalGraphLayoutFactory<GraphHead, Vertex, Edge> factory) {
    Objects.requireNonNull(factory);
    factory.setGradoopFlinkConfig(this);
    logicalGraphFactory.setLayoutFactory(factory);
  }

  /**
   * Sets the layout factory for building layouts that represent a
   * {@link GraphCollection}.
   *
   * @param factory graph collection layout factory
   */
  public void setGraphCollectionLayoutFactory(
    GraphCollectionLayoutFactory<GraphHead, Vertex, Edge> factory) {
    Objects.requireNonNull(factory);
    factory.setGradoopFlinkConfig(this);
    graphCollectionFactory.setLayoutFactory(factory);
  }

  /**
   * Sets the layout factory for building table layouts that represent a
   * {@link TableLogicalGraph}.
   *
   * @param factory table logical graph layout factory
   */
  public void setTableLogicalGraphLayoutFactory(TableLogicalGraphLayoutFactory factory) {
    Objects.requireNonNull(factory);
    factory.setGradoopFlinkConfig(this);
    getTableLogicalGraphFactory().setLayoutFactory(factory);
  }

  /**
   * Sets the layout factory for building table layouts that represent a
   * {@link TableGraphCollection}.
   *
   * @param factory table graph collection layout factory
   */
  public void setTableGraphCollectionLayoutFactory(TableGraphCollectionLayoutFactory factory) {
    Objects.requireNonNull(factory);
    factory.setGradoopFlinkConfig(this);
    getTableGraphCollectionFactory().setLayoutFactory(factory);
  }
}
