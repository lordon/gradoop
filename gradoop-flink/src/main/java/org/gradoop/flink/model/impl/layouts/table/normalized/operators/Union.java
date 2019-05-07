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
package org.gradoop.flink.model.impl.layouts.table.normalized.operators;

import org.apache.flink.table.api.Table;
import org.gradoop.flink.model.impl.layouts.table.normalized.NormalizedTableSet;
import org.gradoop.flink.model.impl.layouts.table.normalized.operators.base.NormalizedCollectionSetOperatorBase;

/**
 * Returns a collection with all logical graphs from two input collections. Graph equality is
 * based on their identifiers. Implemented in normalized layout.
 */
public class Union extends NormalizedCollectionSetOperatorBase {

  @Override
  protected NormalizedTableSet buildInducedTableSet(Table newGraphIds) {
    /*
     In contrast to Difference and Intersection the operator result may contain vertices edges from
     both input collections / table sets. The whole operator logic is overwritten therefore.

     New logic: UNIONs are performed on each table.
      */
    Table newGraphs = computeNewGraphHeads(newGraphIds);
    Table newVerticesGraphs = computeTempVerticesGraphs(newGraphs);
    Table newEdgesGraphs = computeTempEdgesGraphs(newGraphs);
    Table newVertices = computeNewVertices(newVerticesGraphs);
    Table newEdges = computeNewEdges(newEdgesGraphs);
    Table newVertexPropertyValues = computeNewVertexPropertyValues(newVertices);
    Table newEdgePropertyValues = computeNewEdgePropertyValues(newEdges);
    Table newGraphPropertyValues = computeNewGraphPropertyValues(newGraphs);

    return tableSetFactory.fromTables(newVertices, newEdges, newGraphs, newVerticesGraphs,
      newEdgesGraphs, newVertexPropertyValues, newEdgePropertyValues, newGraphPropertyValues);
  }

  @Override
  protected Table computeNewGraphIds() {
    return null;
  }

  @Override
  protected Table computeNewGraphHeads(Table newGraphIds) {
    return firstTableSet.getGraphs()
      .union(otherTableSet.getGraphs());
  }

  /**
   * Computes new vertices_graphs table by performing an UNION on vertices_graphs of both input
   * graph collections
   *
   * @return unioned vertices_graphs tables
   */
  private Table computeNewVerticesGraphs() {
    return firstTableSet.getVerticesGraphs()
      .union(otherTableSet.getVerticesGraphs());
  }

  /**
   * Computes new edges_graphs table by performing an UNION on edges_graphs of both input
   * graph collections
   *
   * @return unioned edges_graphs tables
   */
  private Table computeNewEdgesGraphs() {
    return firstTableSet.getEdgesGraphs()
      .union(otherTableSet.getEdgesGraphs());
  }

  @Override
  protected Table computeNewVertices(Table newGraphs) {
    return firstTableSet.getVertices()
      .union(otherTableSet.getVertices());
  }

  @Override
  protected Table computeNewEdges(Table newGraphs) {
    return firstTableSet.getEdges()
      .union(otherTableSet.getEdges());
  }

  @Override
  protected Table computeNewVertexPropertyValues(Table newVertices) {
    return firstTableSet.getVertexPropertyValues()
      .union(otherTableSet.getVertexPropertyValues());
  }

  @Override
  protected Table computeNewEdgePropertyValues(Table newEdges) {
    return firstTableSet.getEdgePropertyValues()
      .union(otherTableSet.getEdgePropertyValues());
  }

  @Override
  protected Table computeNewGraphPropertyValues(Table newGraphs) {
    return firstTableSet.getGraphPropertyValues()
      .union(otherTableSet.getGraphPropertyValues());
  }

}
