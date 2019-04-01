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
package org.gradoop.flink.model.impl.layouts.table.horizontal;

import org.apache.flink.table.api.Table;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.layouts.table.normalized.NormalizedTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.util.TableUtils;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

/**
 * Responsible for creating instances of {@link HorizontalTableSet}
 */
public class HorizontalTableSetFactory extends NormalizedTableSetFactory {

  /**
   * Constructor
   *
   * @param config gradoop configuration
   */
  public HorizontalTableSetFactory(GradoopFlinkConfig config) {
    super(config);
  }

  /**
   * Creates a horizontal tables set from given tables. Each table gets transformed into a
   * queryable result table.
   *
   * @param vertices vertices table
   * @param edges edges table
   * @param graphs graphs table
   * @param verticesGraphs vertices-graphs table
   * @param edgesGraphs edges-graphs table
   * @param propertyTables list of property tables
   * @return new horizontal table set
   */
  public HorizontalTableSet fromTables(Table vertices, Table edges, Table graphs,
    Table verticesGraphs, Table edgesGraphs, List<PropertyTable> propertyTables) {

    HorizontalTableSet tableSet = new HorizontalTableSet();

    tableSet.put(HorizontalTableSet.TABLE_VERTICES,
      transformToQueryableResultTable(vertices));
    tableSet.put(HorizontalTableSet.TABLE_EDGES,
      transformToQueryableResultTable(edges));
    tableSet.put(HorizontalTableSet.TABLE_GRAPHS,
      transformToQueryableResultTable(graphs));
    tableSet.put(HorizontalTableSet.TABLE_VERTICES_GRAPHS,
      transformToQueryableResultTable(verticesGraphs));
    tableSet.put(HorizontalTableSet.TABLE_EDGES_GRAPHS,
      transformToQueryableResultTable(edgesGraphs));

    for (PropertyTable propertyTable : propertyTables) {
      tableSet.put(propertyTable.getPropertyKey(), propertyTable.getTable());
      tableSet.getSchema().addTable(propertyTable.getPropertyKey(), propertyTable.getSchema());
    }

    return tableSet;
  }

  /**
   * Creates a horizontal tables set from given tables. Each table gets transformed into a
   * queryable result table.
   *
   * @param vertices vertices table
   * @param edges edges table
   * @param propertyTables list of property tables
   * @return new horizontal table set
   */
  public HorizontalTableSet fromTables(Table vertices, Table edges,
    List<PropertyTable> propertyTables) {

    HorizontalTableSet tableSet = new HorizontalTableSet();

    tableSet.put(HorizontalTableSet.TABLE_VERTICES,
      transformToQueryableResultTable(vertices));
    tableSet.put(HorizontalTableSet.TABLE_EDGES,
      transformToQueryableResultTable(edges));

    GradoopId newGraphId = GradoopId.get();

    tableSet.put(HorizontalTableSet.TABLE_GRAPHS,
      transformToQueryableResultTable(computeNewGraphHead(newGraphId)));
    tableSet.put(HorizontalTableSet.TABLE_VERTICES_GRAPHS,
      transformToQueryableResultTable(computeNewVerticesGraphs(vertices, newGraphId)));
    tableSet.put(HorizontalTableSet.TABLE_EDGES_GRAPHS,
      transformToQueryableResultTable(computeNewEdgesGraphs(edges, newGraphId)));

    for (PropertyTable propertyTable : propertyTables) {
      tableSet.put(propertyTable.getPropertyKey(), propertyTable.getTable());
      tableSet.getSchema().addTable(propertyTable.getPropertyKey(), propertyTable.getSchema());
    }

    return tableSet;
  }

  /**
   * Returns empty graphs table
   *
   * @return empty graphs table
   */
  public Table createEmptyGraphsTable() {
    return TableUtils.createEmptyTable(config,
      HorizontalTableSet.SCHEMA.getTable(HorizontalTableSet.TABLE_GRAPHS));
  }

}
