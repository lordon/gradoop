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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.table.TableLayout;
import org.gradoop.flink.model.impl.layouts.table.TableBaseLayout;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for all GVE layouts.
 */
public class TableGVELayout extends TableBaseLayout<GVETableSet, GVETableSetFactory>
  implements TableLayout {

  /**
   * Constructor
   *
   * @param tableSet GVE table set
   * @param config gradoop configuration
   */
  public TableGVELayout(GVETableSet tableSet, GradoopFlinkConfig config) {
    super(tableSet, config);
    this.tableSetFactory = new GVETableSetFactory(config);
  }

  @Override
  public DataSet<Vertex> toVertexDataSet() {
    Table vertices = tableSet.getVertices()
      .select(new ExpressionSeqBuilder()
        .field(GVETableSet.FIELD_VERTEX_ID).as(DATASET_EPGM_ELEMENT_ID_FIELD)
        .field(GVETableSet.FIELD_VERTEX_LABEL).as(DATASET_EPGM_ELEMENT_LABEL_FIELD)
        .field(GVETableSet.FIELD_VERTEX_PROPERTIES).as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .field(GVETableSet.FIELD_VERTEX_GRAPH_IDS).as(DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD)
        .buildSeq()
      );
    return config.getTableEnvironment().toDataSet(vertices, Vertex.class);
  }

  @Override
  public DataSet<Edge> toEdgeDataSet() {
    Table edges = tableSet.getEdges()
      .select(new ExpressionSeqBuilder()
        .field(GVETableSet.FIELD_EDGE_ID).as(DATASET_EPGM_ELEMENT_ID_FIELD)
        .field(GVETableSet.FIELD_EDGE_LABEL).as(DATASET_EPGM_ELEMENT_LABEL_FIELD)
        .field(GVETableSet.FIELD_TAIL_ID).as(DATASET_EPGM_EDGE_SOURCE_ID_FIELD)
        .field(GVETableSet.FIELD_HEAD_ID).as(DATASET_EPGM_EDGE_TARGET_ID_FIELD)
        .field(GVETableSet.FIELD_EDGE_PROPERTIES).as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .field(GVETableSet.FIELD_EDGE_GRAPH_IDS).as(DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD)
        .buildSeq()
      );
    return config.getTableEnvironment().toDataSet(edges, Edge.class);
  }

  @Override
  public DataSet<GraphHead> toGraphHeadDataSet() {
    Table graphs = tableSet.getGraphs()
      .select(new ExpressionSeqBuilder()
        .field(GVETableSet.FIELD_GRAPH_ID).as(DATASET_EPGM_ELEMENT_ID_FIELD)
        .field(GVETableSet.FIELD_GRAPH_LABEL).as(DATASET_EPGM_ELEMENT_LABEL_FIELD)
        .field(GVETableSet.FIELD_GRAPH_PROPERTIES).as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .buildSeq()
      );
    return config.getTableEnvironment().toDataSet(graphs, GraphHead.class);
  }
}
