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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Literal;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.TableBaseLayout;
import org.gradoop.flink.io.impl.table.csv.functions.ParseGradoopIdSet;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.TableUtils;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Base64;

/**
 * Responsible for creating instances of {@link GVETableSetFactory}
 */
public class GVETableSetFactory extends BaseTableSetFactory {

  /**
   * Constructor
   *
   * @param config gradoop configuration
   */
  public GVETableSetFactory(GradoopFlinkConfig config) {
    super(config);
  }

  /**
   * Creates a GVE table set from given table. Each table gets transformed into a queryable
   * result table.
   *
   * @param vertices vertices table
   * @param edges edges table
   * @param graphs graphs table
   * @return new GVE table set
   */
  public GVETableSet fromTables(Table vertices, Table edges, Table graphs) {
    GVETableSet tableSet = new GVETableSet();

    tableSet.put(GVETableSet.TABLE_VERTICES, transformToQueryableResultTable(vertices));
    tableSet.put(GVETableSet.TABLE_EDGES, transformToQueryableResultTable(edges));
    tableSet.put(GVETableSet.TABLE_GRAPHS, transformToQueryableResultTable(graphs));

    return tableSet;
  }

  /**
   * Creates a GVE table set from given tables. A new graph table with a single entry is created.
   * Each graph element gets assigned to this new graph. Each table gets transformed into a
   * queryable result table.
   *
   * @param vertices vertices table
   * @param edges edges table
   * @return new GVE table set
   */
  public GVETableSet fromTables(Table vertices, Table edges) {
    GVETableSet tableSet = new GVETableSet();
    GradoopId newGraphId = GradoopId.get();
    GradoopIdSet newGraphIdSet = GradoopIdSet.fromExisting(newGraphId);

    tableSet.put(GVETableSet.TABLE_GRAPHS, computeNewGraphHead(newGraphId));
    tableSet.put(GVETableSet.TABLE_VERTICES, computeNewVertices(vertices, newGraphIdSet));
    tableSet.put(GVETableSet.TABLE_EDGES, computeNewEdges(edges, newGraphIdSet));

    return tableSet;
  }

  /**
   * Returns a new graph table with a single row. This row consists of given graph id, default
   * graph label and empty graph properties.
   *
   * @param newGraphId new graph id
   * @return new graph table with a single row
   */
  private Table computeNewGraphHead(GradoopId newGraphId) {
    GraphHead newGraphHead = new GraphHeadFactory().initGraphHead(newGraphId);
    DataSet<GraphHead> newGraphIdDataSet =
      config.getExecutionEnvironment().fromElements(newGraphHead);

    ExpressionSeqBuilder builder = new ExpressionSeqBuilder()
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_ID_FIELD)
      .as(GVETableSet.FIELD_GRAPH_ID)
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_LABEL_FIELD)
      .as(GVETableSet.FIELD_GRAPH_LABEL)
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
      .as(GVETableSet.FIELD_GRAPH_PROPERTIES);

    return TableUtils.registerDataSet(config.getTableEnvironment(), newGraphIdDataSet,
      builder.buildSeq());
  }

  /**
   * Performs a
   *
   * SELECT vertex_id, vertex_label, LITERAL(newGraphIdSet) AS vertex_graph_ids, vertex_properties
   * FROM vertices
   *
   * with given new graph id.
   *
   * @param vertices vertices table
   * @param newGraphIdSet new graph id set
   * @return new vertices table
   */
  private Table computeNewVertices(Table vertices, GradoopIdSet newGraphIdSet) {
    return vertices.select(new ExpressionSeqBuilder()
      .field(GVETableSet.FIELD_VERTEX_ID)
      .field(GVETableSet.FIELD_VERTEX_LABEL)
      .scalarFunctionCall(new ParseGradoopIdSet(), new Expression[] {
        new Literal(Base64.getEncoder().encodeToString(newGraphIdSet.toByteArray()), Types.STRING())
      })
      .as(GVETableSet.FIELD_VERTEX_GRAPH_IDS)
      .field(GVETableSet.FIELD_VERTEX_PROPERTIES)
      .buildSeq()
    );
  }

  /**
   * Performs a
   *
   * SELECT edge_id, tail_id, head_id, edge_label, LITERAL(newGraphIdSet) AS edge_graph_ids,
   * edge_properties
   * FROM edges
   *
   * with given new graph id.
   *
   * @param edges edges table
   * @param newGraphIdSet new graph id set
   * @return new edges table
   */
  private Table computeNewEdges(Table edges, GradoopIdSet newGraphIdSet) {
    return edges.select(new ExpressionSeqBuilder()
      .field(GVETableSet.FIELD_EDGE_ID)
      .field(GVETableSet.FIELD_TAIL_ID)
      .field(GVETableSet.FIELD_HEAD_ID)
      .field(GVETableSet.FIELD_EDGE_LABEL)
      .scalarFunctionCall(new ParseGradoopIdSet(), new Expression[] {
        new Literal(Base64.getEncoder().encodeToString(newGraphIdSet.toByteArray()), Types.STRING())
      })
      .as(GVETableSet.FIELD_EDGE_GRAPH_IDS)
      .field(GVETableSet.FIELD_EDGE_PROPERTIES)
      .buildSeq()
    );
  }

}
