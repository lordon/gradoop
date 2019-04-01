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
package org.gradoop.flink.model.impl.layouts.table.horizontal.operators;

import org.apache.flink.table.api.Table;
import org.gradoop.flink.model.impl.layouts.table.common.operators.subgraph.TableSubgraphBase;
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSet;
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.horizontal.PropertyTable;
import org.gradoop.flink.model.impl.layouts.table.horizontal.operators.base.HorizontalOperatorUtils;

import java.util.List;

/**
 * Subgraph operator implementation in horizontal layout.
 */
public class Subgraph extends TableSubgraphBase<HorizontalTableSet, HorizontalTableSetFactory> {

  /**
   * Constructor
   *
   * @param vertexLabels vertex labels
   * @param edgeLabels   edge labels
   * @param strategy     strategy
   */
  public Subgraph(String[] vertexLabels, String[] edgeLabels,
    org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy strategy) {
    super(vertexLabels, edgeLabels, strategy);
  }

  @Override
  protected HorizontalTableSet buildInducedTableSet(Table vertices, Table edges) {
    List<PropertyTable> propertyTables = HorizontalOperatorUtils
      .computeNewElementInducedPropertyTables(tableSet, vertices, edges, config);
    return tableSetFactory.fromTables(vertices, edges, propertyTables);
  }
}
