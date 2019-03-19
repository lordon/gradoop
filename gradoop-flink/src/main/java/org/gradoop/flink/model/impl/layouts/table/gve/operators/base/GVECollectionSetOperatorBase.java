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
package org.gradoop.flink.model.impl.layouts.table.gve.operators.base;

import org.apache.flink.table.api.Table;
import org.gradoop.flink.model.impl.layouts.table.common.operators.base.TableCollectionSetOperatorBase;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.table.ExpandGradoopIdSet;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.TableFunctionCallBuilder;

/**
 * Base class for all collection set operator implementations in GVE layout,
 * like Difference, Intersection and Union
 */
public abstract class GVECollectionSetOperatorBase
  extends TableCollectionSetOperatorBase<GVETableSet, GVETableSetFactory> {

  @Override
  protected GVETableSet buildInducedTableSet(Table newGraphIds) {
    Table newGraphHeads = computeNewGraphHeads(newGraphIds);
    Table newVertices = computeNewVertices(newGraphIds);
    Table newEdges = computeNewEdges(newGraphIds);

    return tableSetFactory.fromTables(newVertices, newEdges, newGraphHeads);
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
        .select(new ExpressionSeqBuilder()
          .field(GVETableSet.FIELD_GRAPH_ID).as(tmpFieldName)
          .field(GVETableSet.FIELD_GRAPH_LABEL)
          .field(GVETableSet.FIELD_GRAPH_PROPERTIES)
          .buildSeq()
        )
        .join(newGraphIds, builder
          .field(GVETableSet.FIELD_GRAPH_ID).equalTo(tmpFieldName).toExpression())
    );
  }

  /**
   * Returns new vertices by performing:
   *
   * SELECT DISTINCT vertex_id, vertex_label, vertex_properties, vertex_graph_ids
   * FROM vertices
   * JOIN ExpandGradoopIdSet(vertex_graph_ids) AS vertex_graph_id
   * JOIN newGraphIds ON graph_id = vertex_graph_id
   *
   * Note the result vertices do contain all graph ids weather they are in the result graph
   * collection or not (this is by Gradoop design).
   *
   * @param newGraphIds table of graph ids
   * @return vertices table
   */
  protected Table computeNewVertices(Table newGraphIds) {
    String tmpFieldName = tableEnv.createUniqueAttributeName();

    return firstTableSet.projectToVertices(
      firstTableSet.getVertices()
        .join(new Table(tableEnv, TableFunctionCallBuilder
          .build(new ExpandGradoopIdSet(), tmpFieldName, GVETableSet.FIELD_VERTEX_GRAPH_IDS)
        ))
        .join(newGraphIds, builder
          .field(GVETableSet.FIELD_GRAPH_ID).equalTo(tmpFieldName).toExpression())
        .distinct()
    );
  }

  /**
   * Returns new edges by performing:
   *
   * SELECT DISTINCT edge_id, tail_id, head_id, edge_label, edge_properties, edge_graph_ids
   * FROM edges
   * JOIN ExpandGradoopIdSet(edge_graph_ids) AS edge_graph_id
   * JOIN newGraphIds ON graph_id = edge_graph_id
   *
   * Note the result edges do contain all graph ids weather they are in the result graph
   * collection or not (this is by Gradoop design).
   *
   * @param newGraphIds table of graph ids
   * @return edges table
   */
  protected Table computeNewEdges(Table newGraphIds) {
    String tmpFieldName = tableEnv.createUniqueAttributeName();

    return firstTableSet.projectToEdges(
      firstTableSet.getEdges()
        .join(new Table(tableEnv, TableFunctionCallBuilder
          .build(new ExpandGradoopIdSet(), tmpFieldName, GVETableSet.FIELD_EDGE_GRAPH_IDS)
        ))
        .join(newGraphIds, builder
          .field(GVETableSet.FIELD_GRAPH_ID).equalTo(tmpFieldName).toExpression())
        .distinct()
    );
  }

}
