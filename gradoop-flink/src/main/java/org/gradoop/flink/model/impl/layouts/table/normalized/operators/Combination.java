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
import org.gradoop.flink.model.impl.layouts.table.normalized.operators.base.NormalizedGraphSetOperatorBase;

/**
 * Computes the combined graph from two logical graphs. Implemented in normalized layout.
 */
public class Combination extends NormalizedGraphSetOperatorBase {

  @Override
  protected Table computeNewVertices() {
    return firstTableSet.getVertices()
      .union(otherTableSet.getVertices());
  }

  @Override
  protected Table computeNewEdges(Table newVertices) {
    return firstTableSet.getEdges()
      .union(otherTableSet.getEdges());
  }

  @Override
  protected NormalizedTableSet buildInducedTableSet(Table vertices, Table edges) {
    Table vertexPropertyValues = firstTableSet.getVertexPropertyValues()
      .union(otherTableSet.getVertexPropertyValues());
    Table edgePropertyValues = firstTableSet.getEdgePropertyValues()
      .union(otherTableSet.getEdgePropertyValues());

    return tableSetFactory.fromTables(vertices, edges, vertexPropertyValues, edgePropertyValues);
  }
}
