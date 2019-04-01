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
package org.gradoop.flink.model.impl.layouts.table.normalized.operators;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Literal;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.layouts.table.common.operators.grouping.TableGroupingBase;
import org.gradoop.flink.model.impl.layouts.table.common.operators.grouping.TableGroupingBuilderBase;
import org.gradoop.flink.model.impl.layouts.table.normalized.NormalizedTableSet;
import org.gradoop.flink.model.impl.layouts.table.normalized.NormalizedTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;

/**
 * Implementation of grouping in normalized layout.
 */
public class Grouping extends TableGroupingBase<NormalizedTableSet, NormalizedTableSetFactory> {

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
   * Responsible for building instances of {@link Grouping} implementation in normalized layout
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
  protected NormalizedTableSet performGrouping() {
    // 1. Prepare vertices
    Table preparedVertices = transformToQueryableResultTable(joinVertexPropertyValues());

    // 2. Group vertices by label and/or property values
    Table groupedVertices = transformToQueryableResultTable(
      preparedVertices
      .groupBy(buildVertexGroupExpressions())
      .select(buildVertexProjectExpressions())
    );

    // 3. Derive new super vertices from grouped vertices
    Table newVertices = groupedVertices.select(buildSuperVertexProjectExpressions());

    // 4. Derive new super vertex property values from grouped vertices
    Table newVertexPropertyValues = computeSuperVertexPropertyValues(groupedVertices);

    // 5. Expand a (vertex -> super vertex) mapping
    Table expandedVertices = transformToQueryableResultTable(
      joinVerticesWithGroupedVertices(preparedVertices, groupedVertices)
    );

    // 6. Assign super vertices to edges
    Table edgesWithSupervertices = enrichEdges(
      tableSet.getEdges(),
      expandedVertices
    );

    // 7. Group edges by label and/or property values
    Table groupedEdges = transformToQueryableResultTable(
        joinEdgePropertyValues(edgesWithSupervertices)
        .groupBy(buildEdgeGroupExpressions())
        .select(buildEdgeProjectExpressions())
    );

    // 8. Derive new super edges from grouped edges
    Table newEdges = transformToQueryableResultTable(
      groupedEdges.select(buildSuperEdgeProjectExpressions())
    );

    // 9. Derive new super edge property values from grouped edges
    Table newEdgePropertyValues = computeSuperEdgePropertyValues(groupedEdges);

    return tableSetFactory
      .fromTables(newVertices, newEdges, newVertexPropertyValues, newEdgePropertyValues);
  }

  /**
   * (...( Vertices ⟕ σ_name=grouping_property_1 (VertexPropertyValues))
   *                ⟕ ...
   *                ⟕ σ_name=grouping_property_n (VertexPropertyValues))
   *                ⟕ σ_name=aggregation_property_1 (VertexPropertyValues))
   *                ⟕ ...
   *                ⟕ σ_name=aggregation_property_m (VertexPropertyValues))
   *
   * @return table of vertices with joined property values
   */
  private Table joinVertexPropertyValues() {
    Table vertices = joinGenericPropertyValues(tableSet.getVertices(),
      tableSet.getVertexPropertyValues(),
      vertexGroupingPropertyKeys, vertexGroupingPropertyFieldNames,
      vertexAggregateFunctions, vertexAggregationPropertyFieldNames,
      NormalizedTableSet.FIELD_VERTEX_PROPERTY_NAME, NormalizedTableSet.FIELD_PROPERTY_VERTEX_ID,
      NormalizedTableSet.FIELD_VERTEX_PROPERTY_VALUE, NormalizedTableSet.FIELD_VERTEX_ID);

    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();
    builder.field(NormalizedTableSet.FIELD_VERTEX_ID);
    if (useVertexLabels) {
      builder.field(NormalizedTableSet.FIELD_VERTEX_LABEL);
    }
    for (String groupingPropertyField : vertexGroupingPropertyFieldNames.values()) {
      builder.field(groupingPropertyField);
    }
    for (String aggregationPropertyField : vertexAggregationPropertyFieldNames.values()) {
      builder.field(aggregationPropertyField);
    }

    return vertices.select(builder.buildSeq());
  }

  /**
   * (...( EnrichedEdges ⟕ σ_name=grouping_property_1 (EdgePropertyValues))
   *                     ⟕ ...
   *                     ⟕ σ_name=grouping_property_k (EdgePropertyValues))
   *                     ⟕ σ_name=aggregation_property_1 (EdgePropertyValues))
   *                     ⟕ ...
   *                     ⟕ σ_name=aggregation_property_l (EdgePropertyValues))
   *
   * @param edges edges table
   * @return table of edges with joined property values
   */
  private Table joinEdgePropertyValues(Table edges) {
    edges = joinGenericPropertyValues(edges, tableSet.getEdgePropertyValues(),
      edgeGroupingPropertyKeys, edgeGroupingPropertyFieldNames,
      edgeAggregateFunctions, edgeAggregationPropertyFieldNames,
      NormalizedTableSet.FIELD_EDGE_PROPERTY_NAME, NormalizedTableSet.FIELD_PROPERTY_EDGE_ID,
      NormalizedTableSet.FIELD_EDGE_PROPERTY_VALUE, NormalizedTableSet.FIELD_EDGE_ID);

    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();
    builder.field(NormalizedTableSet.FIELD_EDGE_ID);
    builder.field(NormalizedTableSet.FIELD_TAIL_ID);
    builder.field(NormalizedTableSet.FIELD_HEAD_ID);
    if (useEdgeLabels) {
      builder.field(NormalizedTableSet.FIELD_EDGE_LABEL);
    }
    for (String groupingPropertyField : edgeGroupingPropertyFieldNames.values()) {
      builder.field(groupingPropertyField);
    }
    for (String aggregationPropertyField : edgeAggregationPropertyFieldNames.values()) {
      builder.field(aggregationPropertyField);
    }

    return edges.select(builder.buildSeq());
  }


  /**
   * Collects all expressions the grouped vertex table gets projected to in order to select super
   * vertices
   *
   * { vertex_id, vertex_label }
   *
   * @return project item list as sequel string
   */
  private Seq<Expression> buildSuperVertexProjectExpressions() {
    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();

    builder.field(FIELD_SUPER_VERTEX_ID).as(NormalizedTableSet.FIELD_VERTEX_ID);

    if (useVertexLabels) {
      builder.field(FIELD_SUPER_VERTEX_LABEL).as(NormalizedTableSet.FIELD_VERTEX_LABEL);
    } else {
      builder
        .expression(new Literal(GradoopConstants.DEFAULT_VERTEX_LABEL, Types.STRING()))
        .as(NormalizedTableSet.FIELD_VERTEX_LABEL);
    }

    return builder.buildSeq();
  }

  /**
   * Collects all expressions the grouped edge table gets projected to in order to select super
   * edge
   *
   * { edge_id, tail_id, head_id, edge_label }
   *
   * @return project item list as sequel string
   */
  private Seq<Expression> buildSuperEdgeProjectExpressions() {
    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();

    builder
      .field(FIELD_SUPER_EDGE_ID).as(NormalizedTableSet.FIELD_EDGE_ID)
      .field(NormalizedTableSet.FIELD_TAIL_ID)
      .field(NormalizedTableSet.FIELD_HEAD_ID);

    if (useEdgeLabels) {
      builder.field(NormalizedTableSet.FIELD_EDGE_LABEL);
    } else {
      builder
        .expression(new Literal(GradoopConstants.DEFAULT_EDGE_LABEL, Types.STRING()))
        .as(NormalizedTableSet.FIELD_EDGE_LABEL);
    }

    return builder.buildSeq();
  }

  /**
   * Collects all grouping property values and aggregate values of grouped vertices and
   * transforms them to VertexPropertyValues
   *
   * ∅ ∪ π_{vertex_id, property_1_name, property_1_value}(GroupedVertices)
   *   ∪ ...
   *   ∪ π_{vertex_id, property_n_name, property_n_value}(GroupedVertices)
   *   ∪ π_{vertex_id, aggregate_1_name, aggregate_1}(GroupedVertices)
   *   ∪ ...
   *   ∪ π_{vertex_id, aggregate_m_name, aggregate_m}(GroupedVertices)
   *
   * Example:
   *
   * | super_vertex_id | ... | city | count |
   * |=================|=====|======|=======|
   * | 1               | ... | L    | 8     |
   * | 2               | ... | D    | 6     |
   *
   * -->
   * | vertex_id | property_name | property_value |
   * |===========|===============|================|
   * | 1         | city          | L              |
   * | 2         | city          | D              |
   * | 1         | count         | 8              |
   * | 2         | count         | 6              |
   *
   * @param groupedVertices grouped vertex table
   * @return vertex property values table
   */
  private Table computeSuperVertexPropertyValues(Table groupedVertices) {
    Table newVertexPropertyValues = tableSetFactory.createEmptyVertexPropertyValuesTable();

    newVertexPropertyValues = computeGenericSuperVertexPropertyValues(groupedVertices,
      newVertexPropertyValues, getVertexAggregatedPropertyKeys(),
      vertexAfterAggregationPropertyFieldNames);

    return computeGenericSuperVertexPropertyValues(groupedVertices,
      newVertexPropertyValues, vertexGroupingPropertyKeys, vertexAfterGroupingPropertyFieldNames);
  }

  /**
   * Collects all grouping property values and aggregate values of grouped edges and
   * transforms them to EdgePropertyValues
   *
   * ∅ ∪ π_{edge_id, property_1_name, property_1_value}(GroupedEdges)
   *   ∪ ...
   *   ∪ π_{edge_id, property_k_name, property_k_value}(GroupedEdges)
   *   ∪ π_{edge_id, aggregate_1_name, aggregate_1}(GroupedEdges)
   *   ∪ ...
   *   ∪ π_{edge_id, aggregate_l_name, aggregate_l}(GroupedEdges)
   *
   * Example:
   *
   * | super_edge_id | ... | since | count |
   * |===============|=====|=======|=======|
   * | 1             | ... | 2011  | 7     |
   * | 2             | ... | 2012  | 5     |
   *
   * -->
   *
   * | edge_id | property_name | property_value |
   * |=========|===============|================|
   * | 1       | since         | 2011           |
   * | 2       | since         | 2012           |
   * | 1       | count         | 7              |
   * | 2       | count         | 5              |
   *
   * @param groupedEdges grouped edge table
   * @return edge property values table
   */
  private Table computeSuperEdgePropertyValues(Table groupedEdges) {
    Table newEdgePropertyValues = tableSetFactory.createEmptyEdgePropertyValuesTable();

    newEdgePropertyValues = computeGenericSuperEdgePropertyValues(groupedEdges,
      newEdgePropertyValues, getEdgeAggregatedPropertyKeys(),
      edgeAfterAggregationPropertyFieldNames);

    return computeGenericSuperEdgePropertyValues(groupedEdges, newEdgePropertyValues,
      edgeGroupingPropertyKeys, edgeAfterGroupingPropertyFieldNames);
  }

  //----------------------------------------------------------------------------
  // Internal DRY
  //----------------------------------------------------------------------------


  /**
   * Abstraction of property values joining used in
   * {@link #joinVertexPropertyValues()} and {@link #joinEdgePropertyValues(Table)}
   *
   * Internal DRY use only
   *
   * @param elementTable Vertices ⊕ Edges
   * @param propertyValuesTable vertexPropertyValues ⊕ edgePropertyValues
   * @param groupingPropertyKeys vertexGroupingPropertyKeys ⊕ edgeGroupingPropertyKeys
   * @param groupingFieldNames vertexGroupingFieldNames ⊕ edgeGroupingFieldNames
   * @param aggregateFunctions vertexAggregateFunctions ⊕ edgeAggregateFunctions
   * @param aggregationFieldNames vertexAggregationFieldNames ⊕ edgeAggregationFieldNames
   * @param elementPropertyNameField field name "{vertex ⊕ edge}_property_name"
   * @param propertyElementIdField field name "property_{vertex ⊕ edge}_id"
   * @param elementPropertyValueField field name "{vertex ⊕ edge}_property_value"
   * @param elementIdField field name "{vertex ⊕ edge}_id"
   * @return element table with joined property values
   */
  private Table joinGenericPropertyValues(Table elementTable, Table propertyValuesTable,
    List<String> groupingPropertyKeys, Map<String, String> groupingFieldNames,
    List<AggregateFunction> aggregateFunctions, Map<String, String> aggregationFieldNames,
    String elementPropertyNameField, String propertyElementIdField,
    String elementPropertyValueField, String elementIdField) {

    for (String propertyKey : groupingPropertyKeys) {
      String propertyFieldName = tableEnv.createUniqueAttributeName();

      elementTable = performGenericPropertyValueJoin(elementTable, propertyValuesTable,
        propertyKey, propertyFieldName, elementPropertyNameField, propertyElementIdField,
        elementPropertyValueField, elementIdField);

      groupingFieldNames.put(propertyKey, propertyFieldName);
    }

    for (AggregateFunction aggregateFunction : aggregateFunctions) {
      if (null != aggregateFunction.getPropertyKey()) {
        String propertyKey = aggregateFunction.getPropertyKey();
        String propertyFieldName = tableEnv.createUniqueAttributeName();

        elementTable = performGenericPropertyValueJoin(elementTable, propertyValuesTable,
          propertyKey, propertyFieldName, elementPropertyNameField, propertyElementIdField,
          elementPropertyValueField, elementIdField);

        aggregationFieldNames.put(aggregateFunction.getAggregatePropertyKey(), propertyFieldName);
      }
    }
    return elementTable;
  }

  /**
   * Abstraction of joining a single (Vertex ⊕ Edge) property to (Vertex ⊕ Edge) table
   * {@link
   * #joinGenericPropertyValues(Table, Table, List, Map, List, Map, String, String, String, String)}
   *
   * Either
   *  - Vertices ⟕ σ_name=property_key (VertexPropertyValues))
   * or
   *  - EnrichedEdges ⟕ σ_name=property_key (EdgePropertyValues))
   *
   * Internal DRY use only
   *
   * @param elementTable Vertices ⊕ Edges
   * @param propertyValuesTable vertexPropertyValues ⊕ edgePropertyValues
   * @param propertyKey property key to join
   * @param propertyFieldName field name for new property field
   * @param elementPropertyNameField field name "{vertex ⊕ edge}_property_name"
   * @param propertyElementIdField field name "property_{vertex ⊕ edge}_id"
   * @param elementPropertyValueField field name "{vertex ⊕ edge}_property_value"
   * @param elementIdField field name "{vertex ⊕ edge}_id"
   * @return element table with joined property value
   */
  private Table performGenericPropertyValueJoin(Table elementTable,
    Table propertyValuesTable, String propertyKey, String propertyFieldName,
    String elementPropertyNameField, String propertyElementIdField,
    String elementPropertyValueField, String elementIdField) {

    ExpressionBuilder builder = new ExpressionBuilder();
    String propertyId = tableEnv.createUniqueAttributeName();

    return elementTable
      .leftOuterJoin(
        propertyValuesTable
          .where(builder
            .field(elementPropertyNameField)
            .equalTo(new Literal(propertyKey, Types.STRING()))
            .toExpression()
          )
          .select(new ExpressionSeqBuilder()
            .field(propertyElementIdField)
            .as(propertyId)
            .field(elementPropertyValueField)
            .as(propertyFieldName)
            .buildSeq()
          ),
        builder
          .field(propertyId)
          .equalTo(elementIdField)
          .toExpression()
      );
  }

  /**
   * Implementation of computation of new super vertex property values used in
   * {@link #computeSuperVertexPropertyValues(Table)}
   *
   * For either
   *  - grouped vertex properties
   * or
   *  - aggregated vertex properties
   *
   * Internal DRY use only
   *
   * @param groupedVertices GroupedVertices table
   * @param vertexPropertyValues VertexPropertyValues to union newly computed values
   * @param propertyKeys property keys of (grouped ⊕ aggregated) vertex properties
   * @param fieldNames field names for (grouped ⊕ aggregated) edge properties
   * @return table of new super vertex-property-values
   */
  private Table computeGenericSuperVertexPropertyValues(Table groupedVertices,
    Table vertexPropertyValues, List<String> propertyKeys, Map<String, String> fieldNames) {
    return computeGenericSuperElementPropertyValues(groupedVertices,
      vertexPropertyValues, propertyKeys, fieldNames, FIELD_SUPER_VERTEX_ID,
      NormalizedTableSet.FIELD_PROPERTY_VERTEX_ID, NormalizedTableSet.FIELD_VERTEX_PROPERTY_NAME,
      NormalizedTableSet.FIELD_VERTEX_PROPERTY_VALUE);
  }

  /**
   * Implementation of computation of new super edge property values used in
   * {@link #computeSuperEdgePropertyValues(Table)}
   *
   * For either
   *  - grouped edge properties
   * or
   *  - aggregated edge properties
   *
   * Internal DRY use only
   *
   * @param groupedEdges GroupedEdges table
   * @param edgePropertyValues EdgePropertyValues to union newly computed values
   * @param propertyKeys property keys of (grouped ⊕ aggregated) edge properties
   * @param fieldNames field names for (grouped ⊕ aggregated) edge properties
   * @return table of new super edge-property-values
   */
  private Table computeGenericSuperEdgePropertyValues(Table groupedEdges,
    Table edgePropertyValues, List<String> propertyKeys, Map<String, String> fieldNames) {
    return computeGenericSuperElementPropertyValues(groupedEdges,
      edgePropertyValues, propertyKeys, fieldNames, FIELD_SUPER_EDGE_ID,
      NormalizedTableSet.FIELD_PROPERTY_EDGE_ID, NormalizedTableSet.FIELD_EDGE_PROPERTY_NAME,
      NormalizedTableSet.FIELD_EDGE_PROPERTY_VALUE);
  }

  /**
   * Abstraction of computation of new (vertex ⊕ edge) property values used in
   * {@link #computeSuperVertexPropertyValues(Table)} and
   * {@link #computeSuperEdgePropertyValues(Table)}
   *
   * Internal DRY use only
   *
   * @param groupedElements GroupedVertices ⊕ GroupedEdges
   * @param elementPropertyValues (Vertex ⊕ Edge)PropertyValues to union newly computed values
   * @param propertyKeys property keys of (grouped ⊕ aggregated) (vertex ⊕ edge) properties
   * @param fieldNames field names for (grouped ⊕ aggregated) (vertex ⊕ edge properties
   * @param superElementIdField field name "super_{vertex ⊕ edge}_id"
   * @param propertyElementIdField field name "property_{vertex ⊕ edge}_id"
   * @param elementPropertyNameField field name "{vertex ⊕ edge}_property_name"
   * @param elementPropertyValueField field name "{vertex ⊕ edge}_property_value"
   * @return property values extracted from grouped elements
   */
  private Table computeGenericSuperElementPropertyValues(Table groupedElements,
    Table elementPropertyValues, List<String> propertyKeys, Map<String, String> fieldNames,
    String superElementIdField, String propertyElementIdField, String elementPropertyNameField,
    String elementPropertyValueField) {

    for (String propertyKey : propertyKeys) {
      String propertyFieldName = fieldNames.get(propertyKey);

      elementPropertyValues = elementPropertyValues.union(
        groupedElements.select(new ExpressionSeqBuilder()
          .field(superElementIdField)
          .as(propertyElementIdField)
          .expression(new Literal(propertyKey, Types.STRING()))
          .as(elementPropertyNameField)
          .field(propertyFieldName)
          .as(elementPropertyValueField)
          .buildSeq()
        )
      );
    }
    return elementPropertyValues;
  }

}
