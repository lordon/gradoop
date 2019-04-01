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
package org.gradoop.flink.model.impl.layouts.table.normalized;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Literal;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.io.impl.table.csv.functions.ParseGradoopId;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.TableUtils;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Responsible for creating instances of {@link NormalizedTableSet}
 */
public class NormalizedTableSetFactory extends BaseTableSetFactory {

  /**
   * Constructor
   *
   * @param config gradoop configuration
   */
  public NormalizedTableSetFactory(GradoopFlinkConfig config) {
    super(config);
  }

  /**
   * Creates a normalized tables set from given tables. Each table gets transformed into a
   * queryable result table.
   *
   * @param vertices vertices table
   * @param edges edges table
   * @param graphs graphs table
   * @param verticesGraphs vertices-graphs table
   * @param edgesGraphs edges-graphs table
   * @param vertexPropertyValues vertex-property-values table
   * @param edgePropertyValues edge-property-values table
   * @param graphPropertyValues graph-property-values table
   * @return new normalized table set
   */
  public NormalizedTableSet fromTables(Table vertices,
    Table edges, Table graphs, Table verticesGraphs, Table edgesGraphs, Table vertexPropertyValues,
    Table edgePropertyValues, Table graphPropertyValues) {
    NormalizedTableSet tableSet = new NormalizedTableSet();

    tableSet.put(NormalizedTableSet.TABLE_VERTICES,
      transformToQueryableResultTable(vertices));
    tableSet.put(NormalizedTableSet.TABLE_EDGES,
      transformToQueryableResultTable(edges));
    tableSet.put(NormalizedTableSet.TABLE_GRAPHS,
      transformToQueryableResultTable(graphs));
    tableSet.put(NormalizedTableSet.TABLE_VERTICES_GRAPHS,
      transformToQueryableResultTable(verticesGraphs));
    tableSet.put(NormalizedTableSet.TABLE_EDGES_GRAPHS,
      transformToQueryableResultTable(edgesGraphs));
    tableSet.put(NormalizedTableSet.TABLE_VERTEX_PROPERTY_VALUES,
      transformToQueryableResultTable(vertexPropertyValues));
    tableSet.put(NormalizedTableSet.TABLE_EDGE_PROPERTY_VALUES,
      transformToQueryableResultTable(edgePropertyValues));
    tableSet.put(NormalizedTableSet.TABLE_GRAPH_PROPERTY_VALUES,
      transformToQueryableResultTable(graphPropertyValues));

    return tableSet;
  }

  /**
   * Creates a normalized tables set from given tables. A new graph table with a single entry
   * is created. Each graph element gets assigned to this new graph. Each table gets transformed
   * into a queryable result table.
   *
   * @param vertices vertices table
   * @param edges edges table
   * @param vertexPropertyValues vertex-property-values table
   * @param edgePropertyValues edge-property-values table
   * @return new normalized table set
   */
  public NormalizedTableSet fromTables(Table vertices, Table edges, Table vertexPropertyValues,
    Table edgePropertyValues) {

    GradoopId newGraphId = GradoopId.get();

    Table graphs = computeNewGraphHead(newGraphId);
    Table verticesGraphs = computeNewVerticesGraphs(vertices, newGraphId);
    Table edgesGraphs = computeNewEdgesGraphs(edges, newGraphId);

    return fromTables(vertices, edges, graphs, verticesGraphs, edgesGraphs,
      vertexPropertyValues, edgePropertyValues, createEmptyGraphPropertyValuesTable());
  }

  /**
   * Returns a new graph table with a single row. This row consists of given graph id and default
   * graph label.
   *
   * @param newGraphId new graph id
   * @return new graph table with a single row
   */
  protected Table computeNewGraphHead(GradoopId newGraphId) {
    String newLabel = GradoopConstants.DEFAULT_GRAPH_LABEL;
    Tuple2<GradoopId, String> newGraphTuple = Tuple2.of(newGraphId, newLabel);

    DataSet<Tuple2<GradoopId, String>> newGraphIdDataSet =
      config.getExecutionEnvironment().fromElements(newGraphTuple);

    return config.getTableEnvironment()
      .fromDataSet(newGraphIdDataSet, NormalizedTableSet.SCHEMA
        .commaSeparatedFieldNamesForTable(NormalizedTableSet.TABLE_GRAPHS));
  }

  /**
   * Performs a
   *
   * SELECT vertex_id AS graph_vertex_id, LITERAL(newGraphId) AS vertex_graph_id
   * FROM vertices
   *
   * with given new graph id.
   *
   * @param vertices vertices table
   * @param newGraphId new graph id
   * @return new vertices-graphs table
   */
  protected Table computeNewVerticesGraphs(Table vertices, GradoopId newGraphId) {
    return vertices
      .select(new ExpressionSeqBuilder()
        .field(NormalizedTableSet.FIELD_VERTEX_ID)
        .as(NormalizedTableSet.FIELD_GRAPH_VERTEX_ID)
        .scalarFunctionCall(new ParseGradoopId(), new Expression[] {
          new Literal(newGraphId.toString(), Types.STRING())
        })
        .as(NormalizedTableSet.FIELD_VERTEX_GRAPH_ID)
        .buildSeq()
      );
  }

  /**
   * Performs a
   *
   * SELECT edge_id AS graph_edge_id, LITERAL(newGraphId) AS edge_graph_id
   * FROM vertices
   *
   * with given new graph id.
   *
   * @param edges edges table
   * @param newGraphId new graph id
   * @return new edges-graphs table
   */
  protected Table computeNewEdgesGraphs(Table edges, GradoopId newGraphId) {
    return edges
      .select(new ExpressionSeqBuilder()
        .field(NormalizedTableSet.FIELD_EDGE_ID)
        .as(NormalizedTableSet.FIELD_GRAPH_EDGE_ID)
        .scalarFunctionCall(new ParseGradoopId(), new Expression[] {
          new Literal(newGraphId.toString(), Types.STRING())
        })
        .as(NormalizedTableSet.FIELD_EDGE_GRAPH_ID)
        .buildSeq()
      );
  }

  /**
   * Returns an empty vertex-property-values table
   *
   * @return empty vertex-property-values table
   */
  public Table createEmptyVertexPropertyValuesTable() {
    return TableUtils.createEmptyTable(config,
      NormalizedTableSet.SCHEMA.getTable(NormalizedTableSet.TABLE_VERTEX_PROPERTY_VALUES));
  }

  /**
   * Returns an empty edge-property-values table
   *
   * @return empty edge-property-values table
   */
  public Table createEmptyEdgePropertyValuesTable() {
    return TableUtils.createEmptyTable(config,
      NormalizedTableSet.SCHEMA.getTable(NormalizedTableSet.TABLE_EDGE_PROPERTY_VALUES));
  }

  /**
   * Returns an empty graph-property-values table
   *
   * @return empty graph-property-values table
   */
  public Table createEmptyGraphPropertyValuesTable() {
    return TableUtils.createEmptyTable(config,
      NormalizedTableSet.SCHEMA.getTable(NormalizedTableSet.TABLE_GRAPH_PROPERTY_VALUES));
  }

}
