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
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSet;
import org.gradoop.flink.model.impl.layouts.table.horizontal.PropertyTable;
import org.gradoop.flink.model.impl.layouts.table.horizontal.operators.base.HorizontalCollectionSetOperatorBase;

import java.util.ArrayList;
import java.util.List;

/**
 * Returns a collection with all logical graphs from two input collections. Graph equality is
 * based on their identifiers. Implemented in normalized layout.
 */
public class Union extends HorizontalCollectionSetOperatorBase {

  @Override
  protected HorizontalTableSet buildInducedTableSet(Table newGraphIds) {
    Table newGraphs = computeNewGraphHeads(newGraphIds);
    Table newVerticesGraphs = computeNewVerticesGraphs();
    Table newEdgesGraphs = computeNewEdgesGraphs();
    Table newVertices = computeNewVertices(newVerticesGraphs);
    Table newEdges = computeNewEdges(newEdgesGraphs);

    List<PropertyTable> newPropertyTables =
      computeNewPropertyTables(newGraphs, newVertices, newEdges);

    return tableSetFactory.fromTables(newVertices, newEdges, newGraphs, newVerticesGraphs,
      newEdgesGraphs, newPropertyTables);
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
  protected List<PropertyTable> computeNewPropertyTables(Table newGraphs, Table newVertices,
    Table newEdges) {
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
