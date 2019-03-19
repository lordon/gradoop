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
import org.apache.flink.table.api.TableEnvironment;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;

/**
 * Utils for operators implemented in GVE table layout.
 */
public class GVEOperatorUtils {

  /**
   * Calculates all vertices of first table set which are not contained in vertices table of
   * second table set (based on id) by performing:
   *
   * SELECT vertex_id, vertex_label, vertex_graph_ids, vertex_properties
   * FROM firstTableSet.vertices
   * JOIN (
   *    SELECT vertex_id AS tmp FROM firstTableSet.vertices
   *    MINUS
   *    SELECT vertex_id AS tmp FROM otherTableSet.vertices
   * ) ON vertex_id = tmp
   *
   * @param tableEnv current table environment
   * @param firstTableSet first GVE based table set
   * @param otherTableSet second GVE based table set
   * @return vertices from first table set minus vertices from table set
   */
  public static Table excludeVerticesById(TableEnvironment tableEnv, GVETableSet firstTableSet,
    GVETableSet otherTableSet) {

    String tmpFieldName = tableEnv.createUniqueAttributeName();

    Table newVertexIds = firstTableSet.getVertices()
      .select(GVETableSet.FIELD_VERTEX_ID)
      .minus(otherTableSet.getVertices()
        .select(GVETableSet.FIELD_VERTEX_ID)
      )
      .select(new ExpressionSeqBuilder()
        .field(GVETableSet.FIELD_VERTEX_ID)
        .as(tmpFieldName).buildSeq());

    return firstTableSet.projectToVertices(
      firstTableSet.getVertices()
        .join(newVertexIds, new ExpressionBuilder()
          .field(GVETableSet.FIELD_VERTEX_ID).equalTo(tmpFieldName).toExpression())
    );
  }

  /**
   * Calculates all vertices which are contained in both vertex tables (based on id):
   *
   * SELECT vertex_id, vertex_label, vertex_graph_ids, vertex_properties
   * FROM firstTableSet.vertices
   * JOIN (
   *  SELECT vertex_id AS tmp FROM otherTableSet.vertices
   * ) ON vertex_id = tmp
   *
   * @param tableEnv current table environment
   * @param firstTableSet first GVE based table set
   * @param otherTableSet second GVE based table set
   * @return vertices from first table set intersected with vertices from second table set
   */
  public static Table overlapVerticesById(TableEnvironment tableEnv, GVETableSet firstTableSet,
    GVETableSet otherTableSet) {

    String tmpFieldName = tableEnv.createUniqueAttributeName();

    return firstTableSet.projectToVertices(firstTableSet.getVertices()
      .join(
        otherTableSet.getVertices().select(
          new ExpressionSeqBuilder()
            .field(GVETableSet.FIELD_VERTEX_ID)
            .as(tmpFieldName).buildSeq()),
        new ExpressionBuilder()
          .field(GVETableSet.FIELD_VERTEX_ID).equalTo(tmpFieldName).toExpression()
      )
    );
  }

  /**
   * Calculates all edges which are contained in both edge tables (based on id):
   *
   * SELECT edge_id, tail_id, head_id, edge_label, edge_graph_ids, edge_properties
   * FROM firstTableSet.edges
   * JOIN (
   *  SELECT edge_id AS tmp FROM otherTableSet.edges
   * ) ON edge_id = tmp
   *
   * @param tableEnv current table environment
   * @param firstTableSet first GVE based table set
   * @param otherTableSet second GVE based table set
   * @return edges from first table set intersected with edges from second table set
   */
  public static Table overlapEdgesById(TableEnvironment tableEnv, GVETableSet firstTableSet,
    GVETableSet otherTableSet) {

    String tmpFieldName = tableEnv.createUniqueAttributeName();

    return firstTableSet.projectToEdges(firstTableSet.getEdges()
      .join(
        otherTableSet.getEdges().select(
          new ExpressionSeqBuilder().field(GVETableSet.FIELD_EDGE_ID).as(tmpFieldName).buildSeq()),
        new ExpressionBuilder()
          .field(GVETableSet.FIELD_EDGE_ID).equalTo(tmpFieldName).toExpression()
      )
    );
  }

  /**
   * Calculates all graph ids of first table which are not contained in graph ids of second table
   * set.
   *
   * @param firstTableSet first GVE based table set
   * @param otherTableSet second GVE based table set
   * @return graph ids of first table set minus graph ids from second table set
   */
  public static Table graphIdDifference(GVETableSet firstTableSet, GVETableSet otherTableSet) {
    return firstTableSet.getGraphs()
      .select(GVETableSet.FIELD_GRAPH_ID)
      .minus(
        otherTableSet.getGraphs()
          .select(GVETableSet.FIELD_GRAPH_ID)
      );
  }

  /**
   * Calculates all graph ids which are contained in both table sets.
   *
   * @param firstTableSet first GVE based table set
   * @param otherTableSet second GVE based table set
   * @return graph ids of first table set intersected with graph ids from second table set
   */
  public static Table graphIdIntersection(GVETableSet firstTableSet, GVETableSet otherTableSet) {
    return firstTableSet.getGraphs()
      .select(GVETableSet.FIELD_GRAPH_ID)
      .intersect(
        otherTableSet.getGraphs()
          .select(GVETableSet.FIELD_GRAPH_ID)
      );
  }
}
