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
package org.gradoop.flink.model.impl.layouts.table.common.operators.base;

import org.apache.flink.table.api.Table;

import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.gradoop.flink.model.api.layouts.table.TableLayout;
import org.gradoop.flink.model.api.operators.Operator;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.TableUtils;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for all operators in table layouts based on GVE layout
 * (there need to be at least three tables: vertices, edges, graphs)
 *
 * @param <TS> table set type
 * @param <TSF> table set factory type
 */
public abstract class TableOperatorBase<TS extends GVETableSet, TSF extends BaseTableSetFactory>
  implements Operator {

  /**
   * Table set factory
   */
  protected TSF tableSetFactory;

  /**
   * Configuration
   */
  protected GradoopFlinkConfig config;

  /**
   * Flink Table Environment
   */
  protected BatchTableEnvironment tableEnv;

  /**
   * Helper for building flink table expressions
   */
  protected ExpressionBuilder builder;

  /**
   * Transform given table to a queryable result which is not recalculated on each reference.
   *
   * @param table table to make queryable
   * @return queryable table
   */
  protected Table transformToQueryableResultTable(Table table) {
    return TableUtils.transformToQueryableResultTable(tableEnv, table);
  }

  /**
   * Computes vertex induced edges by performing
   *
   *  (Edges ⋈ Vertices on head_id=vertex_id)
   *         ⋈ Vertices on tail_id=vertex_id)
   *
   * @param tableSet table set
   * @param originalEdges original edges table
   * @param inducingVertices inducing vertices table
   * @return vertex induced edges table
   */
  protected Table computeNewVertexInducedEdges(TS tableSet, Table originalEdges,
    Table inducingVertices) {
    String newId1 = tableEnv.createUniqueAttributeName();
    String newId2 = tableEnv.createUniqueAttributeName();

    return tableSet.projectToEdges(
      originalEdges
        .join(inducingVertices
          .select(new ExpressionSeqBuilder()
            .field(GVETableSet.FIELD_VERTEX_ID).as(newId1)
            .buildSeq()
          ), builder
          .field(GVETableSet.FIELD_TAIL_ID)
          .equalTo(newId1)
          .toExpression()
        ).join(inducingVertices
          .select(new ExpressionSeqBuilder()
            .field(GVETableSet.FIELD_VERTEX_ID).as(newId2)
            .buildSeq()
          ), builder
          .field(GVETableSet.FIELD_HEAD_ID)
          .equalTo(newId2)
          .toExpression()
        )
    );

  }

  /**
   * Computes edge induced vertices by performing
   *
   * (Vertices ⋈ Edges on vertex_id=head_id)
   * ∪
   * (Vertices ⋈ Edges on vertex_id=tail_id)
   *
   * @param tableSet table set
   * @param originalVertices original vertices table
   * @param inducingEdges inducing edges table
   * @return edge induced vertices table
   */
  protected Table computeNewEdgeInducedVertices(TS tableSet, Table originalVertices,
    Table inducingEdges) {
    return tableSet.projectToVertices(
      originalVertices
        .join(inducingEdges, builder
          .field(GVETableSet.FIELD_VERTEX_ID).equalTo(GVETableSet.FIELD_HEAD_ID).toExpression())
    ).union(tableSet.projectToVertices(
      originalVertices
        .join(inducingEdges, builder
          .field(GVETableSet.FIELD_VERTEX_ID).equalTo(GVETableSet.FIELD_TAIL_ID).toExpression())
    ));
  }

  /**
   * Sets some internal references for easy access in operator implementation
   *
   * @param tableLayout any table layout of a table graph collection or logical graph
   */
  protected void registerTools(TableLayout tableLayout) {
    tableSetFactory = (TSF) tableLayout.getTableSetFactory();
    config = tableLayout.getConfig();
    tableEnv = config.getTableEnvironment();
    builder = new ExpressionBuilder();
  }

}
