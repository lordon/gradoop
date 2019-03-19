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
import org.gradoop.flink.model.impl.layouts.table.TableSet;

/**
 * Interface for all factories of {@link TableLogicalGraphLayout} using Flink's Table-API
 * @param <T> table set type
 */
public interface TableLogicalGraphLayoutFactory<T extends TableSet>
  extends TableBaseLayoutFactory<T> {

  /**
   * Returns a logical graph layout derived from given table set
   * @param tableSet table set
   * @return logical graph layout
   */
  TableLogicalGraphLayout fromTableSet(TableSet tableSet);

  /**
   * Transforms given GVE dataset's into a logical graph layout based on table set defined in
   * this layout
   *
   * @param graphHeads dataset of {@link GraphHead}
   * @param vertices dataset of {@link Vertex}
   * @param edges dataset of {@link Edge}
   * @return table set
   */
  TableLogicalGraphLayout fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges);

  /**
   * Transforms given GVE dataset's into a logical graph layout based on table set defined in
   * this layout
   *
   * @param vertices dataset of {@link Vertex}
   * @return table set
   */
  TableLogicalGraphLayout fromDataSets(DataSet<Vertex> vertices);

  /**
   * Transforms given GVE dataset's into a logical graph layout based on table set defined in
   * this layout
   *
   * @param vertices dataset of {@link Vertex}
   * @param edges dataset of {@link Edge}
   * @return table set
   */
  TableLogicalGraphLayout fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges);

  /**
   * Creates an empty graph layout.
   *
   * @return empty graph layout
   */
  TableLogicalGraphLayout createEmptyGraph();
}
