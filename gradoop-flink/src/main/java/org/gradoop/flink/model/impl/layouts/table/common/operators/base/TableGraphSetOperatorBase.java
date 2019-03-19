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
package org.gradoop.flink.model.impl.layouts.table.common.operators.base;

import org.apache.flink.table.api.Table;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;

/**
 * Base class for table based logical graph set operator implementations, like Combination,
 * Exclusion and Overlap
 *
 * @param <TS> table set type
 * @param <TSF> table set factory type
 */
public abstract class TableGraphSetOperatorBase<
  TS extends GVETableSet,
  TSF extends BaseTableSetFactory>
  extends TableBinaryGraphToGraphOperatorBase<TS, TSF> {

  @Override
  protected TableLogicalGraph computeNewLogicalGraph() {
    Table newVertices = computeNewVertices();
    Table newEdges = computeNewEdges(newVertices);

    TS tableSet = buildInducedTableSet(newVertices, newEdges);
    return config.getTableLogicalGraphFactory().fromTableSet(tableSet);
  }

  /**
   * Computes new set of vertices based on vertex sets of
   *
   * @return table of new vertices
   */
  protected abstract Table computeNewVertices();

  /**
   * Computes new set of edges based on newly computed vertices and/or other tables of
   * {@link this#firstTableSet} and {@link this#otherTableSet}
   *
   * @param newVertices table of newly computed vertices
   * @return table of new edges
   */
  protected abstract Table computeNewEdges(Table newVertices);

  /**
   * Computes new table set of result logicl graph based on the newly computed vertices and edges
   *
   * @param vertices table of newly computed vertices
   * @param edges table of newly computed edges
   * @return table set of result logical graph
   */
  protected abstract TS  buildInducedTableSet(Table vertices, Table edges);

}
