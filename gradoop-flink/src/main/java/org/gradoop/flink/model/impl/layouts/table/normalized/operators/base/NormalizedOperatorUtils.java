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
import org.gradoop.flink.model.impl.layouts.table.normalized.NormalizedTableSet;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionBuilder;

/**
 * Utils for operators implemented in normalized layout.
 */
public class NormalizedOperatorUtils {

  /**
   * Returns a new vertex-property-values table by joining given vertex-property-values with
   * given inducing vertices
   *
   * @param tableSet table set instance
   * @param vertexPropertyValues vertex-property-values table
   * @param inducingVertices table of inducing vertices
   * @return vertex-property-values table
   */
  public static Table computeNewVertexInducedPropertyValues(NormalizedTableSet tableSet,
    Table vertexPropertyValues, Table inducingVertices) {
    return tableSet.projectToVertexPropertyValues(
      vertexPropertyValues.join(inducingVertices, new ExpressionBuilder()
        .field(NormalizedTableSet.FIELD_VERTEX_ID)
        .equalTo(NormalizedTableSet.FIELD_PROPERTY_VERTEX_ID)
        .toExpression()
      )
    );
  }

  /**
   * Returns a new edge-property-values table by joining given edge-property-values with
   * given inducing edges
   *
   * @param tableSet table set instance
   * @param edgePropertyValues edge-property-values table
   * @param inducingEdges table of inducing edges
   * @return edge-property-values table
   */
  public static Table computeNewEdgeInducedPropertyValues(NormalizedTableSet tableSet,
    Table edgePropertyValues, Table inducingEdges) {
    return tableSet.projectToEdgePropertyValues(
      edgePropertyValues.join(inducingEdges, new ExpressionBuilder()
        .field(NormalizedTableSet.FIELD_EDGE_ID)
        .equalTo(NormalizedTableSet.FIELD_PROPERTY_EDGE_ID)
        .toExpression()
      )
    );
  }

  /**
   * Returns a new graph-property-values table by joining given graph-property-values with
   * given inducing graphs
   *
   * @param tableSet table set instance
   * @param graphPropertyValues graph-property-values table
   * @param inducingGraphs table of inducing graphs
   * @return graph-property-values table
   */
  public static Table computeNewGraphInducedPropertyValues(NormalizedTableSet tableSet,
    Table graphPropertyValues, Table inducingGraphs) {
    return tableSet.projectToGraphPropertyValues(
      graphPropertyValues.join(inducingGraphs, new ExpressionBuilder()
        .field(NormalizedTableSet.FIELD_GRAPH_ID)
        .equalTo(NormalizedTableSet.FIELD_PROPERTY_GRAPH_ID)
        .toExpression()
      )
    );
  }

}
