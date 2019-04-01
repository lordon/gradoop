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
import org.gradoop.flink.model.impl.layouts.table.common.operators.base.TableCollectionSetOperatorBase;
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSet;
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.horizontal.PropertyTable;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;

import java.util.List;

/**
 * Base class for all collection set operator implementations in horizontal layout,
 * like Difference, Intersection and Union
 */
public abstract class HorizontalCollectionSetOperatorBase
  extends TableCollectionSetOperatorBase<HorizontalTableSet, HorizontalTableSetFactory> {

  @Override
  protected HorizontalTableSet buildInducedTableSet(Table newGraphIds) {
    Table newGraphs = computeNewGraphHeads(newGraphIds);
    Table newVerticesGraphs = firstTableSet.getVerticesGraphs();
    Table newEdgesGraphs = firstTableSet.getEdgesGraphs();
    Table newVertices = computeNewVertices(computeTempVerticesGraphs(newGraphs));
    Table newEdges = computeNewEdges(computeTempEdgesGraphs(newGraphs));

    List<PropertyTable> newPropertyTables =
      computeNewPropertyTables(newGraphs, newVertices, newEdges);

    return tableSetFactory.fromTables(newVertices, newEdges, newGraphs, newVerticesGraphs,
      newEdgesGraphs, newPropertyTables);
  }

  /**
   * Computes new PropertyTables based on given elements by joining each PropertyTable of first
   * table set with Union of all element's id's.
   *
   * @param newGraphs graphs table
   * @param newVertices vertices table
   * @param newEdges edges table
   * @return list of property tables joined with given element tables
   */
  protected List<PropertyTable> computeNewPropertyTables(Table newGraphs, Table newVertices,
    Table newEdges) {
    return HorizontalOperatorUtils.computeNewElementInducedPropertyTables(firstTableSet,
      newGraphs, newVertices, newEdges, config);
  }

  /**
   * Returns new graphs table by joining graphs of first table set with given table of graph ids
   *
   * @param newGraphIds table of graph ids
   * @return graphs table
   */
  protected Table computeNewGraphHeads(Table newGraphIds) {
    String tmpFieldName = tableEnv.createUniqueAttributeName();
    return firstTableSet.projectToGraphs(
      firstTableSet.getGraphs()
        .join(newGraphIds
            .select(new ExpressionSeqBuilder()
              .field(HorizontalTableSet.FIELD_GRAPH_ID).as(tmpFieldName)
              .buildSeq()),
          builder.field(HorizontalTableSet.FIELD_GRAPH_ID).equalTo(tmpFieldName).toExpression())
    );
  }

  /**
   * Returns a temporary vertices-graphs table by joining vertices-graphs of first table set with
   * given table of graphs
   *
   * @param newGraphs table of graphs
   * @return vertices-graphs table
   */
  protected Table computeTempVerticesGraphs(Table newGraphs) {
    return firstTableSet.projectToVerticesGraphs(
      firstTableSet.getVerticesGraphs()
        .join(newGraphs, builder
          .field(HorizontalTableSet.FIELD_GRAPH_ID)
          .equalTo(HorizontalTableSet.FIELD_VERTEX_GRAPH_ID).toExpression())
    );
  }

  /**
   * Returns a temporary edges-graphs table by joining edges-graphs of first table set with given
   * table of graphs
   *
   * @param newGraphs table of graphs
   * @return edges-graphs table
   */
  protected Table computeTempEdgesGraphs(Table newGraphs) {
    return firstTableSet.projectToEdgesGraphs(
      firstTableSet.getEdgesGraphs()
        .join(newGraphs, builder
          .field(HorizontalTableSet.FIELD_GRAPH_ID)
          .equalTo(HorizontalTableSet.FIELD_EDGE_GRAPH_ID).toExpression())
    );
  }

  /**
   * Returns a new vertices table by joining vertices of first table set with given table of
   * vertices-graphs
   *
   * @param newVerticesGraphs table of vertices-graphs
   * @return vertices table
   */
  protected Table computeNewVertices(Table newVerticesGraphs) {
    Table vertexIds = newVerticesGraphs
      .select(HorizontalTableSet.FIELD_GRAPH_VERTEX_ID).distinct();
    return firstTableSet.projectToVertices(
      firstTableSet.getVertices()
        .join(vertexIds, builder
          .field(HorizontalTableSet.FIELD_VERTEX_ID)
          .equalTo(HorizontalTableSet.FIELD_GRAPH_VERTEX_ID).toExpression())
    );
  }

  /**
   * Returns a new edges table by joining edges of first table set with given table of
   * edges-graphs
   *
   * @param newEdgesGraphs table of edges-graphs
   * @return edges table
   */
  protected Table computeNewEdges(Table newEdgesGraphs) {
    Table edgeIds = newEdgesGraphs
      .select(HorizontalTableSet.FIELD_GRAPH_EDGE_ID).distinct();
    return firstTableSet.projectToEdges(firstTableSet.getEdges()
      .join(edgeIds, builder
        .field(HorizontalTableSet.FIELD_EDGE_ID)
        .equalTo(HorizontalTableSet.FIELD_GRAPH_EDGE_ID).toExpression())
    );
  }
}
