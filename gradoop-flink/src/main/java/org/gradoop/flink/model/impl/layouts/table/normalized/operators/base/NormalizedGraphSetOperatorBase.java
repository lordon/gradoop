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
package org.gradoop.flink.model.impl.layouts.table.normalized.operators.base;

import org.apache.flink.table.api.Table;
import org.gradoop.flink.model.impl.layouts.table.common.operators.base.TableGraphSetOperatorBase;
import org.gradoop.flink.model.impl.layouts.table.normalized.NormalizedTableSet;
import org.gradoop.flink.model.impl.layouts.table.normalized.NormalizedTableSetFactory;

/**
 * Base class for all graph set operator implementations in normalized layout,
 * like Combination, Exclusion and Overlap
 */
public abstract class NormalizedGraphSetOperatorBase
  extends TableGraphSetOperatorBase<NormalizedTableSet, NormalizedTableSetFactory> {

  @Override
  protected NormalizedTableSet buildInducedTableSet(Table vertices, Table edges) {
    Table newVertexPropertyValues =
      NormalizedOperatorUtils.computeNewVertexInducedPropertyValues(
        firstTableSet, firstTableSet.getVertexPropertyValues(), vertices);
    Table newEdgePropertyValues =
      NormalizedOperatorUtils.computeNewEdgeInducedPropertyValues(
        firstTableSet, firstTableSet.getEdgePropertyValues(), edges);

    return tableSetFactory
      .fromTables(vertices, edges, newVertexPropertyValues, newEdgePropertyValues);
  }
}
