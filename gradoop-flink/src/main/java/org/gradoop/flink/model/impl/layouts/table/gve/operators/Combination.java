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
package org.gradoop.flink.model.impl.layouts.table.gve.operators;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.expressions.Expression;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.EmptyGradoopIdSet;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;
import org.gradoop.flink.model.impl.layouts.table.gve.operators.base.GVEGraphSetOperatorBase;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import scala.collection.Seq;

/**
 * Computes the combined graph from two logical graphs. Implemented in GVE table layout.
 */
public class Combination extends GVEGraphSetOperatorBase {

  @Override
  protected Table computeNewVertices() {
    // Leave out graph_ids from UNION
    Seq<Expression> projectExpressions = new ExpressionSeqBuilder()
      .field(GVETableSet.FIELD_VERTEX_ID)
      .field(GVETableSet.FIELD_VERTEX_LABEL)
      .field(GVETableSet.FIELD_VERTEX_PROPERTIES)
      .scalarFunctionCall(new EmptyGradoopIdSet()).as(GVETableSet.FIELD_VERTEX_GRAPH_IDS)
      .buildSeq();

    return firstTableSet.projectToVertices(
      otherTableSet.getVertices().select(projectExpressions)
        .union(firstTableSet.getVertices().select(projectExpressions))
    );
  }

  @Override
  protected Table computeNewEdges(Table newVertices) {
    // Leave out graph_ids from UNION
    Seq<Expression> projectExpressions = new ExpressionSeqBuilder()
      .field(GVETableSet.FIELD_EDGE_ID)
      .field(GVETableSet.FIELD_TAIL_ID)
      .field(GVETableSet.FIELD_HEAD_ID)
      .field(GVETableSet.FIELD_EDGE_LABEL)
      .field(GVETableSet.FIELD_EDGE_PROPERTIES)
      .scalarFunctionCall(new EmptyGradoopIdSet()).as(GVETableSet.FIELD_EDGE_GRAPH_IDS)
      .buildSeq();

    return firstTableSet.projectToEdges(
      otherTableSet.getEdges().select(projectExpressions)
        .union(firstTableSet.getEdges().select(projectExpressions))
    );
  }
}
