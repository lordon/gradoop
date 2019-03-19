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
package org.gradoop.flink.model.api.layouts.table;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.layouts.table.TableSet;

/**
 * Interface for all factories of {@link TableGraphCollectionLayout} using Flink's Table-API
 * @param <T> table set type
 */
public interface TableGraphCollectionLayoutFactory<T extends TableSet> extends TableBaseLayoutFactory<T> {

  /**
   * Returns a graph collection layout derived from given table set
   * @param tableSet table set
   * @return graph collection layout
   */
  TableGraphCollectionLayout fromTableSet(TableSet tableSet);

  /**
   * Transforms given GVE dataset's into a graph collection layout based on table set defined in
   * this layout
   *
   * @param graphHeads dataset of {@link GraphHead}
   * @param vertices dataset of {@link Vertex}
   * @param edges dataset of {@link Edge}
   * @return graph collection layout
   */
  TableGraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges);

  /**
   * Transforms given GVE dataset's into a graph collection layout based on table set defined in
   * this layout
   *
   * @param graphHeads dataset of {@link GraphHead}
   * @param vertices dataset of {@link Vertex}
   * @return graph collection layout
   */
  TableGraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices);

  /**
   * Creates a graph collection layout from a given logical graph.
   *
   * @param logicalGraphLayout  input graph
   * @return 1-element graph collection layout
   */
  TableGraphCollectionLayout fromGraph(TableLogicalGraph logicalGraphLayout);

  /**
   * Creates a graph collection layout from multiple given logical graphs.
   *
   * @param logicalGraphLayout  input graphs
   * @return graph collection
   */
  TableGraphCollectionLayout fromGraphs(TableLogicalGraph... logicalGraphLayout);

  /**
   * Creates an empty graph collection layout.
   *
   * @return empty graph collection layout
   */
  TableGraphCollectionLayout createEmptyCollection();

}
