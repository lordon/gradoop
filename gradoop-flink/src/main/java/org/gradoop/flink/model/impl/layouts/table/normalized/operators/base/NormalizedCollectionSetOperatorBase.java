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
import org.gradoop.flink.model.impl.layouts.table.common.operators.base.TableCollectionSetOperatorBase;
import org.gradoop.flink.model.impl.layouts.table.normalized.NormalizedTableSet;
import org.gradoop.flink.model.impl.layouts.table.normalized.NormalizedTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;

/**
 * Base class for all collection set operator implementations in normalized layout,
 * like Difference, Intersection and Union
 */
public abstract class NormalizedCollectionSetOperatorBase
  extends TableCollectionSetOperatorBase<NormalizedTableSet, NormalizedTableSetFactory> {

  @Override
  protected NormalizedTableSet buildInducedTableSet(Table newGraphIds) {
    if (null != newGraphIds) {
      newGraphIds = transformToQueryableResultTable(newGraphIds);
    }

    Table newGraphs = transformToQueryableResultTable(computeNewGraphHeads(newGraphIds));
    Table newVerticesGraphs = firstTableSet.getVerticesGraphs();
    Table newEdgesGraphs = firstTableSet.getEdgesGraphs();
    Table newVertices =
      transformToQueryableResultTable(computeNewVertices(newGraphIds));
    Table newEdges =
      transformToQueryableResultTable(computeNewEdges(newGraphIds));
    Table newVertexPropertyValues = computeNewVertexPropertyValues(newVertices);
    Table newEdgePropertyValues = computeNewEdgePropertyValues(newEdges);
    Table newGraphPropertyValues = computeNewGraphPropertyValues(newGraphs);

    return tableSetFactory.fromTables(newVertices, newEdges, newGraphs, newVerticesGraphs,
      newEdgesGraphs, newVertexPropertyValues, newEdgePropertyValues, newGraphPropertyValues);
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
              .field(NormalizedTableSet.FIELD_GRAPH_ID).as(tmpFieldName)
              .buildSeq()),
          builder.field(NormalizedTableSet.FIELD_GRAPH_ID).equalTo(tmpFieldName).toExpression())
    );
  }

  /**
   * Returns a new vertices table by joining vertices of first table set with given table of
   * vertices-graphs
   *
   * @param newGraphIds table of graph ids
   * @return vertices table
   */
  protected Table computeNewVertices(Table newGraphIds) {
    Table vertexIds = firstTableSet.getVerticesGraphs()
      .join(newGraphIds, builder
        .field(NormalizedTableSet.FIELD_GRAPH_ID)
        .equalTo(NormalizedTableSet.FIELD_VERTEX_GRAPH_ID).toExpression()
      )
      .select(NormalizedTableSet.FIELD_GRAPH_VERTEX_ID).distinct();

    return firstTableSet.projectToVertices(
      firstTableSet.getVertices()
        .join(vertexIds, builder
          .field(NormalizedTableSet.FIELD_VERTEX_ID)
          .equalTo(NormalizedTableSet.FIELD_GRAPH_VERTEX_ID).toExpression())
    );
  }

  /**
   * Returns a new edges table by joining edges of first table set with given table of
   * edges-graphs
   *
   * @param newGraphIds table of graph ids
   * @return edges table
   */
  protected Table computeNewEdges(Table newGraphIds) {
    Table edgeIds = firstTableSet.getEdgesGraphs()
      .join(newGraphIds, builder
        .field(NormalizedTableSet.FIELD_GRAPH_ID)
        .equalTo(NormalizedTableSet.FIELD_EDGE_GRAPH_ID).toExpression()
      )
      .select(NormalizedTableSet.FIELD_GRAPH_EDGE_ID).distinct();

    return firstTableSet.projectToEdges(firstTableSet.getEdges()
      .join(edgeIds, builder
        .field(NormalizedTableSet.FIELD_EDGE_ID)
        .equalTo(NormalizedTableSet.FIELD_GRAPH_EDGE_ID).toExpression())
    );
  }

  /**
   * Returns a new vertex-property-values table by joining vertex-property-values of first table
   * set with given vertices
   *
   * @param newVertices table of vertices
   * @return vertex-property-values table
   */
  protected Table computeNewVertexPropertyValues(Table newVertices) {
    return NormalizedOperatorUtils.computeNewVertexInducedPropertyValues(firstTableSet,
      firstTableSet.getVertexPropertyValues(), newVertices);
  }

  /**
   * Returns a new edge-property-values table by joining edge-property-values of first table set
   * with given edges
   *
   * @param newEdges table of edges
   * @return edge-property-values table
   */
  protected Table computeNewEdgePropertyValues(Table newEdges) {
    return NormalizedOperatorUtils.computeNewEdgeInducedPropertyValues(firstTableSet,
      firstTableSet.getEdgePropertyValues(), newEdges);
  }

  /**
   * Returns a new graph-property-values table by joining graüh-property-values of first table set
   * with given graphs
   *
   * @param newGraphs table of graphs
   * @return graph-property-values table
   */
  protected Table computeNewGraphPropertyValues(Table newGraphs) {
    return NormalizedOperatorUtils.computeNewGraphInducedPropertyValues(firstTableSet,
      firstTableSet.getGraphPropertyValues(), newGraphs);
  }

}
