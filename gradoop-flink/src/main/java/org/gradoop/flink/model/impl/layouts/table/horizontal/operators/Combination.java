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
import org.gradoop.flink.model.impl.layouts.table.horizontal.PropertyTable;
import org.gradoop.flink.model.impl.layouts.table.horizontal.operators.base.HorizontalGraphSetOperatorBase;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes the combined graph from two logical graphs. Implemented in horizontal layout.
 */
public class Combination extends HorizontalGraphSetOperatorBase {
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
  protected List<PropertyTable> computeNewPropertyTables(Table vertices, Table edges) {
    List<PropertyTable> newPropertyTables = new ArrayList<>();

    List<PropertyTable> firstPropertyTables = firstTableSet.getPropertyTables();
    for (PropertyTable propertyTable : firstPropertyTables) {
      Table newPropertyTable = propertyTable.getTable();
      if (otherTableSet.keySet().contains(propertyTable.getPropertyKey())) {
        newPropertyTable =
          newPropertyTable.union(otherTableSet.get(propertyTable.getPropertyKey()));
      }
      newPropertyTables.add(new PropertyTable(propertyTable.getPropertyKey(), newPropertyTable));
    }

    List<PropertyTable> otherPropertyTables = otherTableSet.getPropertyTables();
    for (PropertyTable propertyTable : otherPropertyTables) {
      if (!firstTableSet.keySet().contains(propertyTable.getPropertyKey())) {
        newPropertyTables.add(propertyTable);
      }
    }

    return newPropertyTables;
  }
}
