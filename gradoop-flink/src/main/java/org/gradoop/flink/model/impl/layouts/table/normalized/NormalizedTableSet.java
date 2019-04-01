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

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.expressions.Expression;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.layouts.table.BaseTableSet;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.model.impl.layouts.table.TableSet;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;
import scala.collection.Seq;

/**
 * Normalized table set
 */
public class NormalizedTableSet extends GVETableSet implements BaseTableSet {

  /**
   * Field name of "foreign key" to vertices in vertices-graphs table
   */
  public static final String FIELD_GRAPH_VERTEX_ID = "graph_vertex_id";
  /**
   * Field name of "foreign key" to graphs in vertices-graphs table
   */
  public static final String FIELD_VERTEX_GRAPH_ID = "vertex_graph_id";
  /**
   * Field name of "foreign key" to edges in edges-graphs table
   */
  public static final String FIELD_GRAPH_EDGE_ID = "graph_edge_id";
  /**
   * Field name of "foreign key" to graphs in edges-graphs table
   */
  public static final String FIELD_EDGE_GRAPH_ID = "edge_graph_id";
  /**
   * Field name of "foreign key" to vertices in vertex-property-values table
   */
  public static final String FIELD_PROPERTY_VERTEX_ID = "property_vertex_id";
  /**
   * Field name of property name in vertex-property-values table
   */
  public static final String FIELD_VERTEX_PROPERTY_NAME = "vertex_property_name";
  /**
   * Field name of property value in vertex-property-values table
   */
  public static final String FIELD_VERTEX_PROPERTY_VALUE = "vertex_property_value";
  /**
   * Field name of "foreign key" to edges in edge-property-values table
   */
  public static final String FIELD_PROPERTY_EDGE_ID = "property_edge_id";
  /**
   * Field name of property name in edge-property-values table
   */
  public static final String FIELD_EDGE_PROPERTY_NAME = "edge_property_name";
  /**
   * Field name of property value in edge-property-values table
   */
  public static final String FIELD_EDGE_PROPERTY_VALUE = "edge_property_value";
  /**
   * Field name of "foreign key" to graphs in graph-property-values table
   */
  public static final String FIELD_PROPERTY_GRAPH_ID = "property_graph_id";
  /**
   * Field name of property name in graph-property-values table
   */
  public static final String FIELD_GRAPH_PROPERTY_NAME = "graph_property_name";
  /**
   * Field name of property value in graph-property-values table
   */
  public static final String FIELD_GRAPH_PROPERTY_VALUE = "graph_property_value";

  /**
   * Table key of vertices-graphs table
   */
  public static final String TABLE_VERTICES_GRAPHS = "vertices_graphs";
  /**
   * Table key of edges-graphs table
   */
  public static final String TABLE_EDGES_GRAPHS = "edges_graphs";
  /**
   * Table key of vertex-property-values table
   */
  static final String TABLE_VERTEX_PROPERTY_VALUES = "vertex_property_values";
  /**
   * Table key of edge-property-values table
   */
  static final String TABLE_EDGE_PROPERTY_VALUES = "edge_property_values";
  /**
   * Table key of graph-property-values table
   */
  static final String TABLE_GRAPH_PROPERTY_VALUES = "graph_property_values";

  /**
   * Initial table set schema of normalized layout
   */
  static final TableSetSchema SCHEMA = new TableSetSchema(
    ImmutableMap.<String, TableSchema>builder()
    .put(TABLE_VERTICES, new TableSchema.Builder()
      .field(FIELD_VERTEX_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_VERTEX_LABEL, Types.STRING())
      .build()
    )
    .put(TABLE_EDGES, new TableSchema.Builder()
      .field(FIELD_EDGE_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_TAIL_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_HEAD_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_EDGE_LABEL, Types.STRING())
      .build()
    )
    .put(TABLE_GRAPHS, new TableSchema.Builder()
      .field(FIELD_GRAPH_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_GRAPH_LABEL, Types.STRING())
      .build()
    )
    .put(TABLE_VERTICES_GRAPHS, new TableSchema.Builder()
      .field(FIELD_GRAPH_VERTEX_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_VERTEX_GRAPH_ID, TypeInformation.of(GradoopId.class))
      .build()
    )
    .put(TABLE_EDGES_GRAPHS, new TableSchema.Builder()
      .field(FIELD_GRAPH_EDGE_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_EDGE_GRAPH_ID, TypeInformation.of(GradoopId.class))
      .build()
    )
    .put(TABLE_VERTEX_PROPERTY_VALUES, new TableSchema.Builder()
      .field(FIELD_PROPERTY_VERTEX_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_VERTEX_PROPERTY_NAME, Types.STRING())
      .field(FIELD_VERTEX_PROPERTY_VALUE, TypeInformation.of(PropertyValue.class))
      .build()
    )
    .put(TABLE_EDGE_PROPERTY_VALUES, new TableSchema.Builder()
      .field(FIELD_PROPERTY_EDGE_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_EDGE_PROPERTY_NAME, Types.STRING())
      .field(FIELD_EDGE_PROPERTY_VALUE, TypeInformation.of(PropertyValue.class))
      .build()
    )
    .put(TABLE_GRAPH_PROPERTY_VALUES, new TableSchema.Builder()
      .field(FIELD_PROPERTY_GRAPH_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_GRAPH_PROPERTY_NAME, Types.STRING())
      .field(FIELD_GRAPH_PROPERTY_VALUE, TypeInformation.of(PropertyValue.class))
      .build()
    )
    .build()
  );

  /**
   * Constructor
   */
  public NormalizedTableSet() { }

  /**
   * Takes a generic {@link TableSet} as parameter and adds all tables of given table set to the
   * new one
   *
   * @param tableSet existing table set
   */
  public NormalizedTableSet(TableSet tableSet) {
    super(tableSet);
  }

  @Override
  public TableSetSchema getSchema() {
    return SCHEMA;
  }

  @Override
  public Table getVertices() {
    return get(TABLE_VERTICES);
  }

  @Override
  public Table getEdges() {
    return get(TABLE_EDGES);
  }

  @Override
  public Table getGraphs() {
    return get(TABLE_GRAPHS);
  }

  /**
   * Returns vertices-graphs table
   *
   * @return vertices-graphs table
   */
  public Table getVerticesGraphs() {
    return get(TABLE_VERTICES_GRAPHS);
  }

  /**
   * Return edges-graphs table
   *
   * @return edges-graphs table
   */
  public Table getEdgesGraphs() {
    return get(TABLE_EDGES_GRAPHS);
  }

  /**
   * Returns vertex-property-values table
   *
   * @return vertex-property-values table
   */
  public Table getVertexPropertyValues() {
    return get(TABLE_VERTEX_PROPERTY_VALUES);
  }

  /**
   * Returns edge-property-values table
   *
   * @return edge-property-values table
   */
  public Table getEdgePropertyValues() {
    return get(TABLE_EDGE_PROPERTY_VALUES);
  }

  /**
   * Returns graph-property-values table
   *
   * @return graph-property-values table
   */
  public Table getGraphPropertyValues() {
    return get(TABLE_GRAPH_PROPERTY_VALUES);
  }

  @Override
  public Table projectToVertices(Table table) {
    return table.select(buildVerticesProjectExpressions());
  }

  @Override
  public Table projectToEdges(Table table) {
    return table.select(buildEdgesProjectExpressions());
  }

  @Override
  public Table projectToGraphs(Table table) {
    return table.select(buildGraphsProjectExpressions());
  }

  /**
   * Projects a given table with a super set of vertices-graphs fields to those vertices-graphs
   * fields
   *
   * @param table table to project
   * @return projected table
   */
  public Table projectToVerticesGraphs(Table table) {
    return table.select(buildVerticesGraphsProjectExpressions());
  }

  /**
   * Projects a given table with a super set of edges-graphs fields to those vertices-graphs
   * fields
   *
   * @param table table to project
   * @return projected table
   */
  public Table projectToEdgesGraphs(Table table) {
    return table.select(buildEdgesGraphsProjectExpressions());
  }

  /**
   * Projects a given table with a super set of vertex-property-values fields to those
   * vertex-property-values fields
   *
   * @param table table to project
   * @return projected table
   */
  public Table projectToVertexPropertyValues(Table table) {
    return table.select(buildVertexPropertyValuesProjectExpressions());
  }

  /**
   * Projects a given table with a super set of edge-property-values fields to those
   * edge-property-values fields
   *
   * @param table table to project
   * @return projected table
   */
  public Table projectToEdgePropertyValues(Table table) {
    return table.select(buildEdgePropertyValuesProjectExpressions());
  }

  /**
   * Projects a given table with a super set of graph-property-values fields to those
   * graph-property-values fields
   *
   * @param table table to project
   * @return projected table
   */
  public Table projectToGraphPropertyValues(Table table) {
    return table.select(buildGraphPropertyValuesProjectExpressions());
  }

  @Override
  public Seq<Expression> buildVerticesProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_VERTICES);
  }

  @Override
  public Seq<Expression> buildEdgesProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_EDGES);
  }

  @Override
  public Seq<Expression> buildGraphsProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_GRAPHS);
  }

  /**
   * Returns a scala sequence of expressions which can be used to project a table with a super set
   * of vertices-graphs fields to those vertices-graphs fields
   *
   * @return scala sequence of expressions
   */
  private Seq<Expression> buildVerticesGraphsProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_VERTICES_GRAPHS);
  }

  /**
   * Returns a scala sequence of expressions which can be used to project a table with a super set
   * of edges-graphs fields to those edges-graphs fields
   *
   * @return scala sequence of expressions
   */
  private Seq<Expression> buildEdgesGraphsProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_EDGES_GRAPHS);
  }

  /**
   * Returns a scala sequence of expressions which can be used to project a table with a super set
   * of vertex-property-values fields to those vertex-property-values fields
   *
   * @return scala sequence of expressions
   */
  private Seq<Expression> buildVertexPropertyValuesProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_VERTEX_PROPERTY_VALUES);
  }

  /**
   * Returns a scala sequence of expressions which can be used to project a table with a super set
   * of edge-property-values fields to those edge-property-values fields
   *
   * @return scala sequence of expressions
   */
  private Seq<Expression> buildEdgePropertyValuesProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_EDGE_PROPERTY_VALUES);
  }

  /**
   * Returns a scala sequence of expressions which can be used to project a table with a super set
   * of graph-property-values fields to those graph-property-values fields
   *
   * @return scala sequence of expressions
   */
  private Seq<Expression> buildGraphPropertyValuesProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_GRAPH_PROPERTY_VALUES);
  }

}
