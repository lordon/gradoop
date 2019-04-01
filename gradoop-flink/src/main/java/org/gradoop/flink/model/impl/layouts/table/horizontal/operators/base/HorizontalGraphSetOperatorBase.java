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
package org.gradoop.flink.model.impl.layouts.table.horizontal.operators.base;

import org.apache.flink.table.api.Table;
import org.gradoop.flink.model.impl.layouts.table.common.operators.base.TableGraphSetOperatorBase;
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSet;
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.horizontal.PropertyTable;

import java.util.List;

/**
 * Base class for all graph set operator implementations in horizontal layout,
 * like Combination, Exclusion and Overlap
 */
public abstract class HorizontalGraphSetOperatorBase
  extends TableGraphSetOperatorBase<HorizontalTableSet, HorizontalTableSetFactory> {

  @Override
  protected HorizontalTableSet buildInducedTableSet(Table vertices, Table edges) {
    List<PropertyTable> newPropertyTables = computeNewPropertyTables(vertices, edges);

    return tableSetFactory.fromTables(vertices, edges, newPropertyTables);
  }

  /**
   * Computes new property tables based on given vertices and edges.
   *
   * @param vertices vertices table
   * @param edges edges table
   * @return property tables based on given vertices and edges
   */
  protected List<PropertyTable> computeNewPropertyTables(Table vertices, Table edges) {
    return HorizontalOperatorUtils.computeNewElementInducedPropertyTables(firstTableSet, vertices,
      edges, config);
  }
}
