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
package org.gradoop.flink.model.impl.layouts.table.common.operators.grouping;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.expressions.Expression;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.layouts.table.operators.TableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.common.operators.base.TableUnaryGraphToGraphOperatorBase;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.EmptyPropertyValue;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.EmptyPropertyValueIfNull;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.NewGradoopId;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import scala.collection.Seq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for table based grouping implementations which are built upon a table set which
 * extends the GVE table set (there need to be at least three tables: vertices, edges, graphs)
 *
 * @param <TS> table set type
 * @param <TSF> table set factory type
 */
public abstract class TableGroupingBase<TS extends GVETableSet, TSF extends BaseTableSetFactory>
  extends TableUnaryGraphToGraphOperatorBase<TS, TSF> implements TableUnaryGraphToGraphOperator {

  /**
   * Field name for super vertex id
   */
  protected static final String FIELD_SUPER_VERTEX_ID = "super_vertex_id";
  /**
   * Field name for super vertex label
   */
  protected static final String FIELD_SUPER_VERTEX_LABEL = "super_vertex_label";
  /**
   * Field name for super edge
   */
  protected static final String FIELD_SUPER_EDGE_ID = "super_edge_id";

  /**
   * True if vertices shall be grouped using their label.
   */
  protected final boolean useVertexLabels;

  /**
   * True if edges shall be grouped using their label.
   */
  protected final boolean useEdgeLabels;

  /**
   * List of property keys to group vertices by (property_1, ..., property_n)
   */
  protected final List<String> vertexGroupingPropertyKeys;

  /**
   * List of aggregate functions to execute on grouped vertices
   */
  protected final List<AggregateFunction> vertexAggregateFunctions;

  /**
   * List of property keys to group edges by
   */
  protected final List<String> edgeGroupingPropertyKeys;

  /**
   * List of aggregate functions to execute on grouped edges
   */
  protected final List<AggregateFunction> edgeAggregateFunctions;

  /**
   * Field names for properties to group vertices by, i. e. a mapping
   *
   * property_1 -> tmp_a1
   * ...
   * property_n -> tmp_an
   */
  protected Map<String, String> vertexGroupingPropertyFieldNames;

  /**
   * Field names for vertex grouping properties after grouping, i. e. a mapping
   *
   * property_1 -> tmp_c1
   * ...
   * property_n -> tmp_cn
   */
  protected Map<String, String> vertexAfterGroupingPropertyFieldNames;

  /**
   * Field names for properties to group edges by, i. e. a mapping
   *
   * property_1 -> tmp_e1
   * ...
   * property_k -> tmp_ek
   */
  protected Map<String, String> edgeGroupingPropertyFieldNames;

  /**
   * Field names for edge grouping properties after grouping, i. e. a mapping
   *
   * property_1 -> tmp_g1
   * ...
   * property_k -> tmp_gk
   */
  protected Map<String, String> edgeAfterGroupingPropertyFieldNames;

  /**
   * Field names for vertex properties to aggregate, i. e. a mapping
   *
   * property_1 -> tmp_b1
   * ...
   * property_m -> tmp_bm
   */
  protected Map<String, String> vertexAggregationPropertyFieldNames;

  /**
   * Field names for vertex aggregation properties after aggregation, i. e. a mapping
   *
   * property_1 -> tmp_d1
   * ...
   * property_m -> tmp_dm
   */
  protected Map<String, String> vertexAfterAggregationPropertyFieldNames;

  /**
   * Field names for edge properties to aggregate, i. e. a mapping
   *
   * property_1 -> tmp_f1
   * ...
   * property_l -> tmp_fl
   */
  protected Map<String, String> edgeAggregationPropertyFieldNames;

  /**
   * Field names for edge aggregation properties after aggregation, i. e. a mapping
   *
   * property_1 -> tmp_h1
   * ...
   * property_l -> tmp_hl
   */
  protected Map<String, String> edgeAfterAggregationPropertyFieldNames;


  /**
   * Creates grouping operator instance.
   *
   * @param useVertexLabels             group on vertex label true/false
   * @param useEdgeLabels               group on edge label true/false
   * @param vertexGroupingPropertyKeys  list of property keys to group vertices by
   * @param vertexAggregateFunctions    aggregate functions to execute on grouped vertices
   * @param edgeGroupingPropertyKeys    list of property keys to group edges by
   * @param edgeAggregateFunctions      aggregate functions to execute on grouped edges
   */
  public TableGroupingBase(
    boolean useVertexLabels,
    boolean useEdgeLabels,
    List<String> vertexGroupingPropertyKeys,
    List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingPropertyKeys,
    List<AggregateFunction> edgeAggregateFunctions
  ) {
    this.useVertexLabels                          = useVertexLabels;
    this.useEdgeLabels                            = useEdgeLabels;
    this.vertexGroupingPropertyKeys               = vertexGroupingPropertyKeys;
    this.vertexAggregateFunctions                 = vertexAggregateFunctions;
    this.edgeGroupingPropertyKeys                 = edgeGroupingPropertyKeys;
    this.edgeAggregateFunctions                   = edgeAggregateFunctions;
    this.vertexGroupingPropertyFieldNames         = new HashMap<>();
    this.vertexAfterGroupingPropertyFieldNames    = new HashMap<>();
    this.edgeGroupingPropertyFieldNames           = new HashMap<>();
    this.edgeAfterGroupingPropertyFieldNames      = new HashMap<>();
    this.vertexAggregationPropertyFieldNames      = new HashMap<>();
    this.vertexAfterAggregationPropertyFieldNames = new HashMap<>();
    this.edgeAggregationPropertyFieldNames        = new HashMap<>();
    this.edgeAfterAggregationPropertyFieldNames   = new HashMap<>();
  }

  @Override
  protected TableLogicalGraph computeNewLogicalGraph() {
    TS tableSet = performGrouping();
    return config.getTableLogicalGraphFactory().fromTableSet(tableSet);
  }

  /**
   * Perform grouping based on {@link this#tableSet} and put result tables into new table set
   *
   * @return table set of result logical graph
   */
  protected abstract TS performGrouping();

  /**
   * Collects all field names the vertex table gets grouped by
   *
   * { property_1, property_2, ..., property_n, vertex_label }
   *
   * @return scala sequence of expressions
   */
  protected Seq<Expression> buildVertexGroupExpressions() {
    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();

    // tmp_a1, ... , tmp_an
    for (String vertexPropertyKey : vertexGroupingPropertyKeys) {
      builder.field(vertexGroupingPropertyFieldNames.get(vertexPropertyKey));
    }

    // optional: vertex_label
    if (useVertexLabels) {
      builder.field(GVETableSet.FIELD_VERTEX_LABEL);
    }

    return builder.buildSeq();
  }

  /**
   * Collects all expressions the grouped vertex table gets projected to
   *
   * { super_vertex_id,
   *   vertex_label,
   *   property_1, property_2, ..., property_n,
   *   aggregate_1, ... aggregate_m }
   *
   * @return scala sequence of expressions
   */
  protected Seq<Expression> buildVertexProjectExpressions() {
    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();

    // super_vertex_id
    builder
      .scalarFunctionCall(new NewGradoopId())
      .as(FIELD_SUPER_VERTEX_ID);

    // optional: vertex_label
    if (useVertexLabels) {
      builder.field(GVETableSet.FIELD_VERTEX_LABEL).as(FIELD_SUPER_VERTEX_LABEL);
    }

    // tmp_a1 AS tmp_c1, ... , tmp_an AS tmp_cn
    for (String vertexPropertyKey : vertexGroupingPropertyKeys) {
      String fieldNameBeforeGrouping = vertexGroupingPropertyFieldNames.get(vertexPropertyKey);
      String fieldNameAfterGrouping = tableEnv.createUniqueAttributeName();
      builder
        .scalarFunctionCall(new EmptyPropertyValueIfNull(), fieldNameBeforeGrouping)
        .as(fieldNameAfterGrouping);
      vertexAfterGroupingPropertyFieldNames.put(vertexPropertyKey, fieldNameAfterGrouping);
    }

    // AGG(tmp_b1) AS tmp_d1, ... , AGG(tmp_bm) AS tmp_dm
    for (AggregateFunction vertexAggregationFunction : vertexAggregateFunctions) {
      ExpressionBuilder expressionToAggregateBuilder = new ExpressionBuilder();

      if (null != vertexAggregationFunction.getPropertyKey()) {
        // Property aggregation function, e.g. MAX, MIN, SUM
        expressionToAggregateBuilder.field(vertexAggregationPropertyFieldNames
          .get(vertexAggregationFunction.getAggregatePropertyKey()));
      } else {
        // Non-property aggregation function, e.g. COUNT
        expressionToAggregateBuilder.scalarFunctionCall(new EmptyPropertyValue());
      }

      String fieldNameAfterAggregation = tableEnv.createUniqueAttributeName();
      builder
        .aggFunctionCall(vertexAggregationFunction.getTableAggFunction(),
          new Expression[] { expressionToAggregateBuilder.toExpression() })
        .as(fieldNameAfterAggregation);

      vertexAfterAggregationPropertyFieldNames
        .put(vertexAggregationFunction.getAggregatePropertyKey(), fieldNameAfterAggregation);
    }

    return builder.buildSeq();
  }

  /**
   * Joins the original vertex set with grouped vertex set in order to get a
   * vertex_id -> super_vertex_id mapping
   *
   * π_{vertex_id, super_vertex_id}(
   *   π_{vertex_id, vertex_label, property_a1, ..., property_an}(PreparedVertices) ⋈_{
   *     vertex_label = super_vertex_label, property_a1 = property_c1, ... property_an =property_cn
   *   } (
   *     π_{super_vertex_id, super_vertex_label, property_c1, ..., property_cn}(GroupedVertices)
   *   )
   * )
   *
   * @param preparedVertices original prepared vertex table
   * @param groupedVertices grouped vertex table
   * @return vertex - super vertex mapping table
   */
  protected Table joinVerticesWithGroupedVertices(Table preparedVertices, Table groupedVertices) {
    ExpressionBuilder joinPredicateBuilder = new ExpressionBuilder();

    ExpressionSeqBuilder preparedVerticesProjectExpressionsBuilder = new ExpressionSeqBuilder()
      .field(GVETableSet.FIELD_VERTEX_ID);
    ExpressionSeqBuilder groupedVerticesProjectExpressionsBuilder = new ExpressionSeqBuilder()
      .field(FIELD_SUPER_VERTEX_ID);

    for (String vertexPropertyKey : vertexGroupingPropertyKeys) {
      String fieldNameBeforeGrouping = vertexGroupingPropertyFieldNames.get(vertexPropertyKey);
      preparedVerticesProjectExpressionsBuilder
        .scalarFunctionCall(new EmptyPropertyValueIfNull(), fieldNameBeforeGrouping)
        .as(fieldNameBeforeGrouping);

      String fieldNameAfterGrouping = vertexAfterGroupingPropertyFieldNames.get(vertexPropertyKey);
      groupedVerticesProjectExpressionsBuilder.field(fieldNameAfterGrouping);

      joinPredicateBuilder.and(
        new ExpressionBuilder()
          .field(fieldNameBeforeGrouping)
          .equalTo(fieldNameAfterGrouping)
          .toExpression()
      );

    }

    if (useVertexLabels) {
      preparedVerticesProjectExpressionsBuilder.field(GVETableSet.FIELD_VERTEX_LABEL);
      groupedVerticesProjectExpressionsBuilder.field(FIELD_SUPER_VERTEX_LABEL);

      joinPredicateBuilder.and(
        new ExpressionBuilder()
          .field(GVETableSet.FIELD_VERTEX_LABEL)
          .equalTo(FIELD_SUPER_VERTEX_LABEL)
          .toExpression()
      );
    }

    return preparedVertices.
      select(preparedVerticesProjectExpressionsBuilder.buildSeq())
      .join(groupedVertices.select(groupedVerticesProjectExpressionsBuilder.buildSeq()),
        joinPredicateBuilder.toExpression())
      .select(new ExpressionSeqBuilder()
        .field(GVETableSet.FIELD_VERTEX_ID)
        .field(FIELD_SUPER_VERTEX_ID)
        .buildSeq()
      );
  }

  /**
   * Assigns edges to super vertices by replacing head and tail id with corresponding super
   * vertex ids
   *
   * π_{edge_id, new_tail_id, new_head_id, edge_label}(
   * Edges ⋈_{head_id=vertex_id}(π_{vertex_id, super_vertex_id AS new_head_id}(ExpandedVertices))
   *       ⋈_{tail_id=vertex_id}(π_{vertex_id, super_vertex_id AS new_tail_id}(ExpandedVertices))
   * )
   *
   * @param edges table of edges to assign super vertices to
   * @param expandedVertices vertex - super vertex mapping table
   * @param additionalProjectExpressions additional expressions the edges get projected to
   * @return enriched edges table
   */
  protected Table enrichEdges(Table edges, Table expandedVertices,
    Expression ... additionalProjectExpressions) {

    String vertexIdHead = tableEnv.createUniqueAttributeName();
    String superVertexIdHead = tableEnv.createUniqueAttributeName();
    String vertexIdTail = tableEnv.createUniqueAttributeName();
    String superVertexIdTail = tableEnv.createUniqueAttributeName();

    ExpressionSeqBuilder projectExpressionsBuilder = new ExpressionSeqBuilder()
      .field(GVETableSet.FIELD_EDGE_ID)
      .field(superVertexIdTail).as(GVETableSet.FIELD_TAIL_ID)
      .field(superVertexIdHead).as(GVETableSet.FIELD_HEAD_ID)
      .field(GVETableSet.FIELD_EDGE_LABEL);

    for (Expression expression : additionalProjectExpressions) {
      projectExpressionsBuilder.expression(expression);
    }

    return tableSet.getEdges()
      .join(expandedVertices
        .select(new ExpressionSeqBuilder()
          .field(GVETableSet.FIELD_VERTEX_ID).as(vertexIdHead)
          .field(FIELD_SUPER_VERTEX_ID).as(superVertexIdHead)
          .buildSeq()
        ), new ExpressionBuilder()
        .field(GVETableSet.FIELD_HEAD_ID).equalTo(vertexIdHead).toExpression()
      )
      .join(expandedVertices
        .select(new ExpressionSeqBuilder()
          .field(GVETableSet.FIELD_VERTEX_ID).as(vertexIdTail)
          .field(FIELD_SUPER_VERTEX_ID).as(superVertexIdTail)
          .buildSeq()
        ), new ExpressionBuilder()
        .field(GVETableSet.FIELD_TAIL_ID).equalTo(vertexIdTail).toExpression()
      )
      .select(projectExpressionsBuilder.buildSeq()
    );
  }

  /**
   * Collects all field names the edge relation gets grouped by
   *
   * { tail_id, head_id, property_1, property_2, ..., property_k, edge_label }
   *
   * @return scala sequence of expressions
   */
  protected Seq<Expression> buildEdgeGroupExpressions() {
    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();

    // tail_id, head_id
    builder.field(GVETableSet.FIELD_TAIL_ID);
    builder.field(GVETableSet.FIELD_HEAD_ID);

    // tmp_e1, ... , tmp_ek
    for (String edgePropertyKey : edgeGroupingPropertyKeys) {
      builder.field(edgeGroupingPropertyFieldNames.get(edgePropertyKey));
    }

    // optionally: edge_label
    if (useEdgeLabels) {
      builder.field(GVETableSet.FIELD_EDGE_LABEL);
    }

    return builder.buildSeq();
  }

  /**
   * Collects all expressions the grouped edge table gets projected to
   *
   * { super_edge_id,
   *   tail_id,
   *   head_id,
   *   edge_label,
   *   property_1, property_2, ..., property_k,
   *   aggregate_1, ... aggregate_l }
   *
   * @return scala sequence of expressions
   */
  protected Seq<Expression> buildEdgeProjectExpressions() {
    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();

    // super_edge_id, tail_head, head_id
    builder
      .scalarFunctionCall(new NewGradoopId())
      .as(FIELD_SUPER_EDGE_ID)
      .field(GVETableSet.FIELD_TAIL_ID)
      .field(GVETableSet.FIELD_HEAD_ID);

    // optional: edge_label
    if (useEdgeLabels) {
      builder.field(GVETableSet.FIELD_EDGE_LABEL);
    }

    // tmp_e1 AS tmp_g1, ... , tmp_ek AS tmp_gk
    for (String edgePropertyKey : edgeGroupingPropertyKeys) {
      String fieldNameBeforeGrouping = edgeGroupingPropertyFieldNames.get(edgePropertyKey);
      String fieldNameAfterGrouping = tableEnv.createUniqueAttributeName();
      builder
        .scalarFunctionCall(new EmptyPropertyValueIfNull(), fieldNameBeforeGrouping)
        .as(fieldNameAfterGrouping);
      edgeAfterGroupingPropertyFieldNames.put(edgePropertyKey, fieldNameAfterGrouping);
    }

    // AGG(tmp_f1) AS tmp_h1, ... , AGG(tmp_fl) AS tmp_hl
    for (AggregateFunction edgeAggregateFunction : edgeAggregateFunctions) {
      ExpressionBuilder expressionToAggregateBuilder = new ExpressionBuilder();
      if (null != edgeAggregateFunction.getPropertyKey()) {
        // Property aggregation function, e.g. MAX, MIN, SUM
        expressionToAggregateBuilder
          .field(edgeAggregationPropertyFieldNames
            .get(edgeAggregateFunction.getAggregatePropertyKey()));
      } else {
        // Non-property aggregation function, e.g. COUNT
        expressionToAggregateBuilder.scalarFunctionCall(new EmptyPropertyValue());
      }

      String fieldNameAfterAggregation = tableEnv.createUniqueAttributeName();
      builder
        .aggFunctionCall(edgeAggregateFunction.getTableAggFunction(),
        new Expression[]{ expressionToAggregateBuilder.toExpression() })
        .as(fieldNameAfterAggregation);

      edgeAfterAggregationPropertyFieldNames.put(edgeAggregateFunction.getAggregatePropertyKey(),
        fieldNameAfterAggregation);
    }

    return builder.buildSeq();
  }

  /**
   * Returns a list of all super vertex property keys which will hold an aggregate after vertex
   * grouping
   *
   * @return list of all super vertex property keys which will hold an aggregate after vertex
   */
  protected List<String> getVertexAggregatedPropertyKeys() {
    return getAggregatedPropertyKeys(vertexAggregateFunctions);
  }

  /**
   * Returns a list of all super edge property keys which will hold an aggregate after edge grouping
   *
   * @return list of all super edge property keys which will hold an aggregate after edge grouping
   */
  protected List<String> getEdgeAggregatedPropertyKeys() {
    return getAggregatedPropertyKeys(edgeAggregateFunctions);
  }

  /**
   * Returns a list of all property keys which will hold an aggregate after grouping for given
   * list of aggregate functions
   *
   * @param funcs list of aggregate functions
   * @return list of all property keys which will hold an aggregate after grouping
   */
  private List<String> getAggregatedPropertyKeys(List<AggregateFunction> funcs) {
    return funcs.stream()
      .map(AggregateFunction::getAggregatePropertyKey)
      .collect(Collectors.toList());
  }

}
