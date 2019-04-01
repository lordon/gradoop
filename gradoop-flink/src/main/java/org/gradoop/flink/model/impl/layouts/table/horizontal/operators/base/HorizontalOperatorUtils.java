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
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSet;
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.horizontal.PropertyTable;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.TableUtils;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Utils for operators implemented in horizontal layout.
 */
public class HorizontalOperatorUtils {

  /**
   * Calculates a list of property tables containing only property values of given elements
   * (vertices, edges, graphs) by joining property tables of given table set with given element
   * tables:
   *
   * - for each property_table in table set:
   *  SELECT property_element_id, property_value
   *  FROM property_table
   *  JOIN (
   *    SELECT graph_id AS tmp FROM graphs
   *    UNION (SELECT vertex_id AS tmp FROM vertices)
   *    UNION (SELECT edge_id AS tmp FROM edges)
   *  ) tmp_table ON property_element_id = tmp
   *
   * @param tableSet table set instance
   * @param graphs graphs table to join with property tables
   * @param vertices vertices table to join with property tables
   * @param edges edges table to join with property tables
   * @param config current gradoop configuration
   * @return list of property tables of given table set joined with element tables
   */
  public static List<PropertyTable> computeNewElementInducedPropertyTables(
    HorizontalTableSet tableSet, Table graphs, Table vertices, Table edges,
    GradoopFlinkConfig config) {

    BatchTableEnvironment tableEnv = config.getTableEnvironment();
    ExpressionBuilder builder = new ExpressionBuilder();

    String elementIdTmpField = tableEnv.createUniqueAttributeName();
    Table newElementIds =
      graphs.select(new ExpressionSeqBuilder()
        .field(HorizontalTableSet.FIELD_GRAPH_ID)
        .as(elementIdTmpField)
        .buildSeq())
        .unionAll(vertices.select(new ExpressionSeqBuilder()
          .field(HorizontalTableSet.FIELD_VERTEX_ID)
          .as(elementIdTmpField)
          .buildSeq()))
        .unionAll(edges.select(new ExpressionSeqBuilder()
          .field(HorizontalTableSet.FIELD_EDGE_ID)
          .as(elementIdTmpField)
          .buildSeq()));

    newElementIds = TableUtils.transformToQueryableResultTable(tableEnv, newElementIds);

    List<PropertyTable> newPropertyTables = new ArrayList<>();

    for (PropertyTable propertyTable : tableSet.getPropertyTables()) {
      Table newPropertyTable = propertyTable.getTable()
        .join(newElementIds, builder
          .field(propertyTable.getElementIdFieldName())
          .equalTo(elementIdTmpField)
          .toExpression()
        )
        .select(new ExpressionSeqBuilder()
          .field(propertyTable.getElementIdFieldName())
          .field(propertyTable.getPropertyValueFieldName())
          .buildSeq()
        );

      newPropertyTables.add(new PropertyTable(propertyTable.getPropertyKey(), newPropertyTable));
    }

    return newPropertyTables;
  }

  /**
   * Calculates a list of property tables containing only property values of given elements
   * (vertices, edges) by joining property tables of given table set with given element
   * tables:
   *
   * - for each property_table in table set:
   *  SELECT property_element_id, property_value
   *  FROM property_table
   *  JOIN (
   *    SELECT vertex_id AS tmp FROM vertices
   *    UNION (SELECT edge_id AS tmp FROM edges)
   *  ) tmp_table ON property_element_id = tmp
   *
   * @param tableSet table set instance
   * @param vertices vertices table to join with property tables
   * @param edges edges table to join with property tables
   * @param config current gradoop configuration
   * @return list of property tables of given table set joined with element tables
   */
  public static List<PropertyTable> computeNewElementInducedPropertyTables(
    HorizontalTableSet tableSet, Table vertices, Table edges, GradoopFlinkConfig config) {
    Table emptyGraphs = new HorizontalTableSetFactory(config).createEmptyGraphsTable();
    return computeNewElementInducedPropertyTables(tableSet, emptyGraphs, vertices, edges, config);
  }

}
