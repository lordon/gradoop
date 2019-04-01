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
package org.gradoop.flink.model.impl.layouts.table.horizontal.operators;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Literal;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.layouts.table.common.operators.grouping.TableGroupingBase;
import org.gradoop.flink.model.impl.layouts.table.common.operators.grouping.TableGroupingBuilderBase;
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSet;
import org.gradoop.flink.model.impl.layouts.table.horizontal.HorizontalTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.horizontal.PropertyTable;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of grouping in horizontal layout.
 */
public class Grouping extends TableGroupingBase<HorizontalTableSet, HorizontalTableSetFactory> {

  /**
   * Creates grouping operator instance.
   *
   * @param useVertexLabels            group on vertex label true/false
   * @param useEdgeLabels              group on edge label true/false
   * @param vertexGroupingPropertyKeys list of property keys to group vertices by
   * @param vertexAggregateFunctions   aggregate functions to execute on grouped vertices
   * @param edgeGroupingPropertyKeys   list of property keys to group edges by
   * @param edgeAggregateFunctions     aggregate functions to execute on grouped edges
   */
  public Grouping(boolean useVertexLabels, boolean useEdgeLabels,
    List<String> vertexGroupingPropertyKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingPropertyKeys, List<AggregateFunction> edgeAggregateFunctions) {
    super(useVertexLabels, useEdgeLabels, vertexGroupingPropertyKeys, vertexAggregateFunctions,
      edgeGroupingPropertyKeys, edgeAggregateFunctions);
  }

  /**
   * Responsible for building instances of {@link Grouping} implementation in horizontal layout
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
  protected HorizontalTableSet performGrouping() {
    // 1. Prepare vertices
    Table preparedVertices = transformToQueryableResultTable(joinVertexPropertyTables());

    // 2. Group vertices by label and/or property values
    Table groupedVertices = transformToQueryableResultTable(
      preparedVertices
        .groupBy(buildVertexGroupExpressions())
        .select(buildVertexProjectExpressions())
    );

    // 3. Derive new super vertices from grouped vertices
    Table newVertices = groupedVertices.select(buildSuperVertexProjectExpressions());

    // 4. Derive new super vertex property values from grouped vertices
    List<PropertyTable> newPropertyTables = computeSuperVertexPropertyTables(groupedVertices);

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
      joinEdgePropertyTables(edgesWithSupervertices)
        .groupBy(buildEdgeGroupExpressions())
        .select(buildEdgeProjectExpressions())
    );

    // 8. Derive new super edges from grouped edges
    Table newEdges = transformToQueryableResultTable(
      groupedEdges.select(buildSuperEdgeProjectExpressions())
    );

    // 9. Derive new super edge property values from grouped edges
    newPropertyTables = appendSuperEdgePropertyTables(newPropertyTables, groupedEdges);

    return tableSetFactory
      .fromTables(newVertices, newEdges, newPropertyTables);
  }

  /**
   * (...( Vertices ⟕ (GroupingProperty1))
   *                ⟕ ...
   *                ⟕ (GroupingPropertyN))
   *                ⟕ (AggregationProperty1))
   *                ⟕ ...
   *                ⟕ (AggregationPropertyM))
   *
   * @return table of vertices with joined property values
   */
  private Table joinVertexPropertyTables() {
    Table vertices = joinGenericPropertyTables(tableSet.getVertices(),
      vertexGroupingPropertyKeys, vertexGroupingPropertyFieldNames,
      vertexAggregateFunctions, vertexAggregationPropertyFieldNames,
      HorizontalTableSet.FIELD_VERTEX_ID);

    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();
    builder.field(HorizontalTableSet.FIELD_VERTEX_ID);
    if (useVertexLabels) {
      builder.field(HorizontalTableSet.FIELD_VERTEX_LABEL);
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
   * (...( Enriched ⟕ (GroupingProperty1))
   *                ⟕ ...
   *                ⟕ (GroupingPropertyK))
   *                ⟕ (AggregationProperty1))
   *                ⟕ ...
   *                ⟕ (AggregationPropertyL))
   *
   * @param edges edges table
   * @return table of edges with joined property values
   */
  private Table joinEdgePropertyTables(Table edges) {
    edges = joinGenericPropertyTables(edges,
      edgeGroupingPropertyKeys, edgeGroupingPropertyFieldNames,
      edgeAggregateFunctions, edgeAggregationPropertyFieldNames,
      HorizontalTableSet.FIELD_EDGE_ID);

    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();
    builder.field(HorizontalTableSet.FIELD_EDGE_ID);
    builder.field(HorizontalTableSet.FIELD_TAIL_ID);
    builder.field(HorizontalTableSet.FIELD_HEAD_ID);
    if (useEdgeLabels) {
      builder.field(HorizontalTableSet.FIELD_EDGE_LABEL);
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

    builder.field(FIELD_SUPER_VERTEX_ID).as(HorizontalTableSet.FIELD_VERTEX_ID);

    if (useVertexLabels) {
      builder.field(FIELD_SUPER_VERTEX_LABEL).as(HorizontalTableSet.FIELD_VERTEX_LABEL);
    } else {
      builder
        .expression(new Literal(GradoopConstants.DEFAULT_VERTEX_LABEL, Types.STRING()))
        .as(HorizontalTableSet.FIELD_VERTEX_LABEL);
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
      .field(FIELD_SUPER_EDGE_ID).as(HorizontalTableSet.FIELD_EDGE_ID)
      .field(HorizontalTableSet.FIELD_TAIL_ID)
      .field(HorizontalTableSet.FIELD_HEAD_ID);

    if (useEdgeLabels) {
      builder.field(HorizontalTableSet.FIELD_EDGE_LABEL);
    } else {
      builder
        .expression(new Literal(GradoopConstants.DEFAULT_EDGE_LABEL, Types.STRING()))
        .as(HorizontalTableSet.FIELD_EDGE_LABEL);
    }

    return builder.buildSeq();
  }

  /**
   * Collects all grouping property values and aggregate values of grouped vertices and
   * transforms them into PropertyTables
   *
   * π_{vertex_id, property_1_value}(GroupedVertices)
   * ...
   * π_{vertex_id, property_n_value}(GroupedVertices)
   * π_{vertex_id, aggregate_1}(GroupedVertices)
   * ...
   * π_{vertex_id, aggregate_m}(GroupedVertices)
   *
   * Example:
   *
   * | super_vertex_id | ... | city | count |
   * |=================|=====|======|=======|
   * | 1               | ... | L    | 8     |
   * | 2               | ... | D    | 6     |
   *
   * -->
   * | city_element_id | city_property_value |
   * |=================|=====================|
   * | 1               | L                   |
   * | 2               | D                   |
   *
   * | count_element_id | count_property_value |
   * |==================|======================|
   * | 1                | 8                    |
   * | 2                | 6                    |
   *
   *
   * @param groupedVertices grouped vertex table
   * @return vertex property values table
   */
  private List<PropertyTable> computeSuperVertexPropertyTables(Table groupedVertices) {
    List<PropertyTable> propertyTables = new ArrayList<>();

    propertyTables = computeGenericSuperVertexPropertyTables(groupedVertices, propertyTables,
      getVertexAggregatedPropertyKeys(), vertexAfterAggregationPropertyFieldNames);

    return computeGenericSuperVertexPropertyTables(groupedVertices, propertyTables,
      vertexGroupingPropertyKeys, vertexAfterGroupingPropertyFieldNames);
  }

  /**
   * Collects all grouping property values and aggregate values of grouped edges and
   * transforms them into PropertyTables, i. e. appends them to given existing property tables
   *
   * π_{edge_id, property_1_value}(GroupedEdges)
   * ...
   * π_{edge_id, property_k_value}(GroupedEdges)
   * π_{edge_id, aggregate_1}(GroupedEdges)
   * ...
   * π_{edge_id, aggregate_l}(GroupedEdges)
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
   * | since_element_id | since_property_value |
   * |==================|======================|
   * | 1                | 2011                 |
   * | 2                | 2012                 |
   *
   * | count_element_id | count_property_value |
   * |==================|======================|
   * | 1                | 7                    |
   * | 2                | 5                    |
   *
   * @param existingPropertyTables existing property tables to integrate new property tables with
   * @param groupedEdges grouped edge table
   * @return edge property values table
   */
  private List<PropertyTable>  appendSuperEdgePropertyTables(
    List<PropertyTable> existingPropertyTables, Table groupedEdges) {

    existingPropertyTables = computeGenericSuperEdgePropertyTables(groupedEdges,
      existingPropertyTables, getEdgeAggregatedPropertyKeys(),
      edgeAfterAggregationPropertyFieldNames);

    return computeGenericSuperEdgePropertyTables(groupedEdges, existingPropertyTables,
      edgeGroupingPropertyKeys, edgeAfterGroupingPropertyFieldNames);
  }

  //----------------------------------------------------------------------------
  // Internal DRY
  //----------------------------------------------------------------------------


  /**
   * Abstraction of property values joining used in
   * {@link #joinVertexPropertyTables()} and {@link #joinEdgePropertyTables(Table)}
   *
   * Internal DRY use only
   *
   * @param elementTable Vertices ⊕ Edges
   * @param groupingPropertyKeys vertexGroupingPropertyKeys ⊕ edgeGroupingPropertyKeys
   * @param groupingFieldNames vertexGroupingFieldNames ⊕ edgeGroupingFieldNames
   * @param aggregateFunctions vertexAggregateFunctions ⊕ edgeAggregateFunctions
   * @param aggregationFieldNames vertexAggregationFieldNames ⊕ edgeAggregationFieldNames
   * @param elementIdField field name "{vertex ⊕ edge}_id"
   * @return element table with joined property values
   */
  private Table joinGenericPropertyTables(Table elementTable, List<String> groupingPropertyKeys,
    Map<String, String> groupingFieldNames, List<AggregateFunction> aggregateFunctions,
    Map<String, String> aggregationFieldNames, String elementIdField) {

    for (String propertyKey : groupingPropertyKeys) {
      String propertyFieldName = tableEnv.createUniqueAttributeName();

      elementTable = performGenericPropertyTableJoin(elementTable, propertyKey, propertyFieldName,
        elementIdField);

      groupingFieldNames.put(propertyKey, propertyFieldName);
    }

    for (AggregateFunction aggregateFunction : aggregateFunctions) {
      if (null != aggregateFunction.getPropertyKey()) {
        String propertyFieldName = tableEnv.createUniqueAttributeName();

        elementTable = performGenericPropertyTableJoin(elementTable,
          aggregateFunction.getPropertyKey(), propertyFieldName, elementIdField);

        aggregationFieldNames.put(aggregateFunction.getAggregatePropertyKey(), propertyFieldName);
      }
    }
    return elementTable;
  }

  /**
   * Abstraction of joining a single PropertyTable to (Vertex ⊕ Edge) table
   * {@link #joinGenericPropertyTables(Table, List, Map, List, Map, String)}
   *
   * Either
   *  - Vertices ⟕ Property
   * or
   *  - EnrichedEdges ⟕ Property
   *
   * @param elementTable Vertices ⊕ Edges
   * @param propertyKey property key to join
   * @param propertyFieldName field name for new property field
   * @param elementIdField field name "{vertex ⊕ edge}_id"
   * @return element table with joined property table
   */
  public Table performGenericPropertyTableJoin(Table elementTable, String propertyKey,
    String propertyFieldName, String elementIdField) {
    String elementIdFieldName = tableEnv.createUniqueAttributeName();

    PropertyTable propertyTable = tableSet.getPropertyTables().stream()
      .filter(t -> t.getPropertyKey().equals(propertyKey))
      .findFirst()
      .orElse(PropertyTable.createEmptyPropertyTable(propertyKey, config));

    return elementTable
      .leftOuterJoin(propertyTable.getTable().select(new ExpressionSeqBuilder()
        .field(propertyTable.getElementIdFieldName())
        .as(elementIdFieldName)
        .field(propertyTable.getPropertyValueFieldName())
        .as(propertyFieldName)
        .buildSeq()), builder
        .field(elementIdFieldName)
        .equalTo(elementIdField)
        .toExpression());
  }

  /**
   * Implementation of computation of new super vertex property tables used in
   * {@link #computeSuperVertexPropertyTables(Table)}
   *
   * For either
   *  - grouped vertex properties
   * or
   *  - aggregated vertex properties
   *
   * Internal DRY use only
   *
   * @param groupedVertices GroupedVertices table
   * @param propertyTables list of property tables to add new property tables to
   * @param propertyKeys property keys of (grouped ⊕ aggregated) vertex properties
   * @param fieldNames field names for (grouped ⊕ aggregated) edge properties
   * @return list of property tables
   */
  private List<PropertyTable> computeGenericSuperVertexPropertyTables(Table groupedVertices,
    List<PropertyTable> propertyTables, List<String> propertyKeys, Map<String, String> fieldNames) {
    return computeGenericSuperElementPropertyTables(groupedVertices, propertyTables, propertyKeys,
      fieldNames, FIELD_SUPER_VERTEX_ID);
  }

  /**
   * Implementation of computation of new super edge property values used in
   * {@link #appendSuperEdgePropertyTables(List, Table)}
   *
   * For either
   *  - grouped edge properties
   * or
   *  - aggregated edge properties
   *
   * Internal DRY use only
   *
   * @param groupedEdges GroupedEdges table
   * @param propertyTables list of property tables to add new property tables to
   * @param propertyKeys property keys of (grouped ⊕ aggregated) edge properties
   * @param fieldNames field names for (grouped ⊕ aggregated) edge properties
   * @return list of property tables
   */
  private List<PropertyTable> computeGenericSuperEdgePropertyTables(Table groupedEdges,
    List<PropertyTable> propertyTables, List<String> propertyKeys, Map<String, String> fieldNames) {
    return computeGenericSuperElementPropertyTables(groupedEdges,
      propertyTables, propertyKeys, fieldNames, FIELD_SUPER_EDGE_ID);
  }

  /**
   * Abstraction of computation of new (vertex ⊕ edge) property tables used in
   * {@link #computeGenericSuperVertexPropertyTables(Table, List, List, Map)} and
   * {@link #computeGenericSuperEdgePropertyTables(Table, List, List, Map)}
   *
   * Internal DRY use only
   *
   * @param groupedElements GroupedVertices ⊕ GroupedEdges
   * @param propertyTables list of property tables to add new property tables to
   * @param propertyKeys property keys of (grouped ⊕ aggregated) (vertex ⊕ edge) properties
   * @param fieldNames field names for (grouped ⊕ aggregated) (vertex ⊕ edge properties
   * @param superElementIdField field name "super_{vertex ⊕ edge}_id"
   * @return list of property tables
   */
  private List<PropertyTable> computeGenericSuperElementPropertyTables(Table groupedElements,
    List<PropertyTable> propertyTables, List<String> propertyKeys, Map<String, String> fieldNames,
    String superElementIdField) {

    for (String propertyKey : propertyKeys) {
      PropertyTable propertyTable = propertyTables.stream()
        .filter(t -> t.getPropertyKey().equals(propertyKey))
        .findFirst()
        .orElse(PropertyTable.createEmptyPropertyTable(propertyKey, config));

      if (!propertyTables.contains(propertyTable)) {
        propertyTables.add(propertyTable);
      }

      String propertyFieldName = fieldNames.get(propertyKey);

      propertyTable.setTable(propertyTable.getTable()
        .unionAll(
          groupedElements.select(new ExpressionSeqBuilder()
            .field(superElementIdField)
            .as(propertyTable.getElementIdFieldName())
            .field(propertyFieldName)
            .as(propertyTable.getPropertyValueFieldName())
            .buildSeq()
          )
        )
      );
    }
    return propertyTables;
  }
}
