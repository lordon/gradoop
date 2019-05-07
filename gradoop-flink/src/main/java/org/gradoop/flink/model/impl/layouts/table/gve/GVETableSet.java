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
package org.gradoop.flink.model.impl.layouts.table.gve;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.expressions.Expression;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.layouts.table.BaseTableSet;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.model.impl.layouts.table.TableSet;
import scala.collection.Seq;

/**
 * GVE table set
 */
public class GVETableSet extends TableSet implements BaseTableSet {

  /**
   * Field name of id in vertices table
   */
  public static final String FIELD_VERTEX_ID = "vertex_id";
  /**
   * Field name of label in vertices table
   */
  public static final String FIELD_VERTEX_LABEL = "vertex_label";
  /**
   * Field name of graph ids in vertices table
   */
  public static final String FIELD_VERTEX_GRAPH_IDS = "vertex_graph_ids";
  /**
   * Field name of properties in vertices table
   */
  public static final String FIELD_VERTEX_PROPERTIES = "vertex_properties";
  /**
   * Field name of id in edges table
   */
  public static final String FIELD_EDGE_ID = "edge_id";
  /**
   * Field name of tail id in edges table
   */
  public static final String FIELD_TAIL_ID = "tail_id";
  /**
   * Field name of head id in edges table
   */
  public static final String FIELD_HEAD_ID = "head_id";
  /**
   * Field name of label in edges table
   */
  public static final String FIELD_EDGE_LABEL = "edge_label";
  /**
   * Field name of graph ids in edges table
   */
  public static final String FIELD_EDGE_GRAPH_IDS = "edge_graph_ids";
  /**
   * Field name of properties in edges table
   */
  public static final String FIELD_EDGE_PROPERTIES = "edge_properties";
  /**
   * Field name of id in graphs table
   */
  public static final String FIELD_GRAPH_ID = "graph_id";
  /**
   * Field name of label in graphs table
   */
  public static final String FIELD_GRAPH_LABEL = "graph_label";
  /**
   * Field name of properties in graphs table
   */
  public static final String FIELD_GRAPH_PROPERTIES = "graph_properties";

  /**
   * Table key of vertices table
   */
  public static final String TABLE_VERTICES = "vertices";
  /**
   * Table key of edges table
   */
  public static final String TABLE_EDGES = "edges";
  /**
   * Table key of graphs table
   */
  public static final String TABLE_GRAPHS = "graphs";

  /**
   * Initial table set schema of GVE layout
   */
  static final TableSetSchema SCHEMA = new TableSetSchema(
    ImmutableMap.<String, TableSchema>builder()
      .put(TABLE_VERTICES, new TableSchemaBuilder()
        .field(FIELD_VERTEX_ID, TypeInformation.of(GradoopId.class))
        .field(FIELD_VERTEX_LABEL, Types.STRING())
        .field(FIELD_VERTEX_GRAPH_IDS, TypeInformation.of(GradoopIdSet.class))
        .field(FIELD_VERTEX_PROPERTIES, TypeInformation.of(Properties.class))
        .build()
      )
      .put(TABLE_EDGES, new TableSchemaBuilder()
        .field(FIELD_EDGE_ID, TypeInformation.of(GradoopId.class))
        .field(FIELD_TAIL_ID, TypeInformation.of(GradoopId.class))
        .field(FIELD_HEAD_ID, TypeInformation.of(GradoopId.class))
        .field(FIELD_EDGE_LABEL, Types.STRING())
        .field(FIELD_EDGE_GRAPH_IDS, TypeInformation.of(GradoopIdSet.class))
        .field(FIELD_EDGE_PROPERTIES, TypeInformation.of(Properties.class))
        .build()
      )
      .put(TABLE_GRAPHS, new TableSchemaBuilder()
        .field(FIELD_GRAPH_ID, TypeInformation.of(GradoopId.class))
        .field(FIELD_GRAPH_LABEL, Types.STRING())
        .field(FIELD_GRAPH_PROPERTIES, TypeInformation.of(Properties.class))
        .build()
      )
      .build());

  /**
   * Constructor
   */
  public GVETableSet() { }

  /**
   * Takes a generic {@link TableSet} as parameter and adds all tables of given table set to the
   * new one
   *
   * @param tableSet existing table set
   */
  public GVETableSet(TableSet tableSet) {
    super(tableSet);
  }

  @Override
  public TableSetSchema getSchema() {
    return SCHEMA;
  }

  /**
   * Return vertices table
   *
   * @return vertices table
   */
  public Table getVertices() {
    return get(TABLE_VERTICES);
  }

  /**
   * Returns edges table
   *
   * @return edges table
   */
  public Table getEdges() {
    return get(TABLE_EDGES);
  }

  /**
   * Returns graphs table
   *
   * @return graphs table
   */
  public Table getGraphs() {
    return get(TABLE_GRAPHS);
  }

  /**
   * Projects a given table with a super set of vertices fields to those vertices fields
   *
   * @param table table to project
   * @return projected table
   */
  public Table projectToVertices(Table table) {
    return table.select(buildVerticesProjectExpressions());
  }

  /**
   * Projects a given table with a super set of edges fields to those edges fields
   *
   * @param table table to project
   * @return projected table
   */
  public Table projectToEdges(Table table) {
    return table.select(buildEdgesProjectExpressions());
  }

  /**
   * Projects a given table with a super set of graphs fields to those graphs fields
   *
   * @param table table to project
   * @return projected table
   */
  public Table projectToGraphs(Table table) {
    return table.select(buildGraphsProjectExpressions());
  }

  /**
   * Returns a scala sequence of expressions which can be used to project a table with a super set
   * of vertices fields to those vertices fields
   *
   * @return scala sequence of expressions
   */
  public Seq<Expression> buildVerticesProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_VERTICES);
  }

  /**
   * Returns a scala sequence of expressions which can be used to project a table with a super set
   * of edges fields to those edges fields
   *
   * @return scala sequence of expressions
   */
  public Seq<Expression> buildEdgesProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_EDGES);
  }

  /**
   * Returns a scala sequence of expressions which can be used to project a table with a super set
   * of graphs fields to those graphs fields
   *
   * @return scala sequence of expressions
   */
  public Seq<Expression> buildGraphsProjectExpressions() {
    return SCHEMA.buildProjectExpressions(TABLE_GRAPHS);
  }

}
