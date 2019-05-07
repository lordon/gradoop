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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.RowConstructor;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.layouts.table.common.operators.grouping.TableGroupingBase;
import org.gradoop.flink.model.impl.layouts.table.common.operators.grouping.TableGroupingBuilderBase;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.EmptyGradoopIdSet;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.EmptyProperties;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.ExtractPropertyValue;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.ToProperties;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;

/**
 * Implementation of grouping in GVE layout.
 */
public class Grouping extends TableGroupingBase<GVETableSet, GVETableSetFactory> {

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
  public Grouping(boolean useVertexLabels, boolean useEdgeLabels,
    List<String> vertexGroupingPropertyKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingPropertyKeys, List<AggregateFunction> edgeAggregateFunctions) {
    super(useVertexLabels, useEdgeLabels, vertexGroupingPropertyKeys, vertexAggregateFunctions,
      edgeGroupingPropertyKeys, edgeAggregateFunctions);
  }

  /**
   * Responsible for building instances of {@link Grouping} implementation in GVE layout
   */
  public static class GroupingBuilder extends TableGroupingBuilderBase {
    @Override
    public Grouping build() {
      return new Grouping(
        useVertexLabel,
        useEdgeLabel,
        vertexPropertyKeys,
        vertexAggregateFunctions,
        edgePropertyKeys,
        edgeAggregateFunctions
      );
    }
  }

  @Override
  protected GVETableSet performGrouping() {
    tableSetFactory = new GVETableSetFactory(config);

    // 1. Prepare vertices
    Table preparedVertices = transformToQueryableResultTable(
      extractVertexPropertyValuesAsColumns()
    );

    // 2. Group vertices by label and/or property values
    Table groupedVertices = transformToQueryableResultTable(
      preparedVertices
      .groupBy(buildVertexGroupExpressions())
      .select(buildVertexProjectExpressions())
    );

    // 3. Derive new super vertices
    Table newVertices = groupedVertices
      .select(buildSuperVertexProjectExpressions());

    // 4. Expand a (vertex -> super vertex) mapping
    Table expandedVertices = transformToQueryableResultTable(
      joinVerticesWithGroupedVertices(preparedVertices, groupedVertices)
    );

    // 5. Assign super vertices to edges
    Table edgesWithSupervertices = enrichEdges(tableSet.getEdges(), expandedVertices);

    // 6. Group edges by label and/or property values
    Table groupedEdges = edgesWithSupervertices
        .groupBy(buildEdgeGroupExpressions())
        .select(buildEdgeProjectExpressions());

    // 7. Derive new super edges from grouped edges
    Table newEdges = transformToQueryableResultTable(
      groupedEdges.select(buildSuperEdgeProjectExpressions())
    );

    return tableSetFactory.fromTables(newVertices, newEdges);
  }

  /**
   * Projects needed property values from properties instance into own fields for each property.
   *
   * @return prepared vertices table
   */
  private Table extractVertexPropertyValuesAsColumns() {
    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();

    // vertex_id
    builder.field(GVETableSet.FIELD_VERTEX_ID);

    // optionally: vertex_label
    if (useVertexLabels) {
      builder.field(GVETableSet.FIELD_VERTEX_LABEL);
    }

    // grouping_property_1 AS tmp_a1, ... , grouping_property_n AS tmp_an
    for (String propertyKey : vertexGroupingPropertyKeys) {
      String propertyFieldName = tableEnv.createUniqueAttributeName();
      builder
        .scalarFunctionCall(new ExtractPropertyValue(propertyKey),
          GVETableSet.FIELD_VERTEX_PROPERTIES)
        .as(propertyFieldName);
      vertexGroupingPropertyFieldNames.put(propertyKey, propertyFieldName);
    }

    // property_to_aggregate_1 AS tmp_b1, ... , property_to_aggregate_m AS tmp_bm
    for (AggregateFunction aggregateFunction : vertexAggregateFunctions) {
      if (null != aggregateFunction.getPropertyKey()) {
        String propertyFieldName = tableEnv.createUniqueAttributeName();
        builder.scalarFunctionCall(new ExtractPropertyValue(aggregateFunction.getPropertyKey()),
          GVETableSet.FIELD_VERTEX_PROPERTIES).as(propertyFieldName);
        vertexAggregationPropertyFieldNames
          .put(aggregateFunction.getAggregatePropertyKey(), propertyFieldName);
      }
    }

    return tableSet.getVertices().select(builder.buildSeq());
  }

  /**
   * Collects all expressions the grouped vertex table gets projected to in order to select super
   * vertices
   *
   * { vertex_id, vertex_label, vertex_graph_ids, vertex_properties }
   *
   * @return scala sequence of expressions
   */
  private Seq<Expression> buildSuperVertexProjectExpressions() {
    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();

    // vertex_id
    builder.field(FIELD_SUPER_VERTEX_ID).as(GVETableSet.FIELD_VERTEX_ID);

    // vertex_label
    if (useVertexLabels) {
      builder.field(FIELD_SUPER_VERTEX_LABEL).as(GVETableSet.FIELD_VERTEX_LABEL);
    } else {
      builder
        .expression(new Literal(GradoopConstants.DEFAULT_VERTEX_LABEL, Types.STRING()))
        .as(GVETableSet.FIELD_VERTEX_LABEL);
    }

    // vertex_graph_ids
    builder.scalarFunctionCall(new EmptyGradoopIdSet()).as(GVETableSet.FIELD_VERTEX_GRAPH_IDS);

    // grouped_properties + aggregated_properties -> vertex_properties
    ExpressionSeqBuilder propertyKeysAndFieldsBuilder = new ExpressionSeqBuilder();
    addPropertyKeyValueExpressions(
      propertyKeysAndFieldsBuilder,
      vertexGroupingPropertyKeys, vertexAfterGroupingPropertyFieldNames
    );
    addPropertyKeyValueExpressions(
      propertyKeysAndFieldsBuilder,
      getVertexAggregatedPropertyKeys(), vertexAfterAggregationPropertyFieldNames
    );

    if (propertyKeysAndFieldsBuilder.isEmpty()) {
      builder.scalarFunctionCall(new EmptyProperties());
    } else {
      builder.scalarFunctionCall(new ToProperties(),
        new Expression[] { new RowConstructor(propertyKeysAndFieldsBuilder.buildSeq()) });
    }

    builder.as(GVETableSet.FIELD_VERTEX_PROPERTIES);

    return builder.buildSeq();
  }

  /**
   * Collects all expressions the grouped edge table gets projected to in order to select super
   * edges
   *
   * { edge_id, tail_id, head_id, edge_label, edge_graph_ids, edge_properties }
   *
   * @return scala sequence of expressions
   */
  private Seq<Expression> buildSuperEdgeProjectExpressions() {
    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();

    // edge_id, tail_id, head_id
    builder
      .field(FIELD_SUPER_EDGE_ID).as(GVETableSet.FIELD_EDGE_ID)
      .field(GVETableSet.FIELD_TAIL_ID)
      .field(GVETableSet.FIELD_HEAD_ID);

    // edge_label
    if (useEdgeLabels) {
      builder.field(GVETableSet.FIELD_EDGE_LABEL);
    } else {
      builder
        .expression(new Literal(GradoopConstants.DEFAULT_EDGE_LABEL, Types.STRING()))
        .as(GVETableSet.FIELD_EDGE_LABEL);
    }

    // edge_graph_ids
    builder.scalarFunctionCall(new EmptyGradoopIdSet()).as(GVETableSet.FIELD_EDGE_GRAPH_IDS);

    // grouped_properties + aggregated_properties -> edge_properties
    ExpressionSeqBuilder propertyKeysAndFieldsBuilder = new ExpressionSeqBuilder();
    addPropertyKeyValueExpressions(
      propertyKeysAndFieldsBuilder,
      edgeGroupingPropertyKeys, edgeAfterGroupingPropertyFieldNames
    );
    addPropertyKeyValueExpressions(
      propertyKeysAndFieldsBuilder,
      getEdgeAggregatedPropertyKeys(), edgeAfterAggregationPropertyFieldNames
    );

    if (propertyKeysAndFieldsBuilder.isEmpty()) {
      builder.scalarFunctionCall(new EmptyProperties());
    } else {
      builder.scalarFunctionCall(new ToProperties(),
        new Expression[]{ new RowConstructor(propertyKeysAndFieldsBuilder.buildSeq())});
    }
    builder.as(GVETableSet.FIELD_EDGE_PROPERTIES);

    return builder.buildSeq();
  }

  @Override
  protected Table enrichEdges(Table edges, Table expandedVertices,
    Expression... additionalProjectExpressions) {

    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();

    // grouping_property_1 AS tmp_e1, ... , grouping_property_k AS tmp_ek
    for (String propertyKey : edgeGroupingPropertyKeys) {
      String propertyColumnName = tableEnv.createUniqueAttributeName();
      builder
        .scalarFunctionCall(new ExtractPropertyValue(propertyKey),
          GVETableSet.FIELD_EDGE_PROPERTIES)
        .as(propertyColumnName);
      edgeGroupingPropertyFieldNames.put(propertyKey, propertyColumnName);
    }

    // property_to_aggregate_1 AS tmp_f1, ... , property_to_aggregate_l AS tmp_fl
    for (AggregateFunction aggregateFunction : edgeAggregateFunctions) {
      if (null != aggregateFunction.getPropertyKey()) {
        String propertyColumnName = tableEnv.createUniqueAttributeName();
        builder.scalarFunctionCall(new ExtractPropertyValue(aggregateFunction.getPropertyKey()),
          GVETableSet.FIELD_EDGE_PROPERTIES)
          .as(propertyColumnName);
        edgeAggregationPropertyFieldNames.put(aggregateFunction.getAggregatePropertyKey(),
          propertyColumnName);
      }
    }

    return super.enrichEdges(edges, expandedVertices,
      builder.buildList().toArray(new Expression[0]));
  }

  /**
   * Takes an expression sequence builder and adds following expressions for each of given
   * property keys to the builder:
   *
   * LITERAL('property_key_1'), field_name_of_property_1, ... , LITERAL('property_key_n'),
   * field_name_of_property_n
   *
   * @param builder expression sequence builder to add expressions to
   * @param propertyKeys property keys
   * @param fieldNameMap map of field names properties
   */
  private void addPropertyKeyValueExpressions(ExpressionSeqBuilder builder,
    List<String> propertyKeys, Map<String, String> fieldNameMap) {
    for (String propertyKey : propertyKeys) {
      builder.expression(new Literal(propertyKey, Types.STRING()));
      builder.resolvedField(fieldNameMap.get(propertyKey),
        TypeInformation.of(PropertyValue.class));
    }
  }

}
