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
import org.gradoop.flink.model.api.layouts.table.TableBaseLayoutFactory;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;
import org.gradoop.flink.model.impl.layouts.table.TableBaseLayout;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.TableUtils;

/**
 * Base class for all layout factories of GVE layout
 */
public abstract class TableGVEBaseLayoutFactory extends BaseFactory
  implements TableBaseLayoutFactory<GVETableSet> {

  @Override
  public TableSetSchema getSchema() {
    return GVETableSet.SCHEMA;
  }

  @Override
  public GVETableSet tableSetfromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    GVETableSetFactory tableSetFactory = new GVETableSetFactory(getConfig());

    ExpressionSeqBuilder graphFieldsBuilder = new ExpressionSeqBuilder()
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_ID_FIELD)
      .as(GVETableSet.FIELD_GRAPH_ID)
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_LABEL_FIELD)
      .as(GVETableSet.FIELD_GRAPH_LABEL)
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
      .as(GVETableSet.FIELD_GRAPH_PROPERTIES);

    Table graphsTable = TableUtils.registerDataSet(getConfig().getTableEnvironment(), graphHeads,
      graphFieldsBuilder.buildSeq());

    ExpressionSeqBuilder vertexFieldsBuilder = new ExpressionSeqBuilder()
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_ID_FIELD)
      .as(GVETableSet.FIELD_VERTEX_ID)
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_LABEL_FIELD)
      .as(GVETableSet.FIELD_VERTEX_LABEL)
      .field(TableBaseLayout.DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD)
      .as(GVETableSet.FIELD_VERTEX_GRAPH_IDS)
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
      .as(GVETableSet.FIELD_VERTEX_PROPERTIES);

    Table verticesTable = TableUtils.registerDataSet(getConfig().getTableEnvironment(), vertices,
      vertexFieldsBuilder.buildSeq());

    ExpressionSeqBuilder edgeFieldsBuilder = new ExpressionSeqBuilder()
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_ID_FIELD)
      .as(GVETableSet.FIELD_EDGE_ID)
      .field(TableBaseLayout.DATASET_EPGM_EDGE_SOURCE_ID_FIELD)
      .as(GVETableSet.FIELD_TAIL_ID)
      .field(TableBaseLayout.DATASET_EPGM_EDGE_TARGET_ID_FIELD)
      .as(GVETableSet.FIELD_HEAD_ID)
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_LABEL_FIELD)
      .as(GVETableSet.FIELD_EDGE_LABEL)
      .field(TableBaseLayout.DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD)
      .as(GVETableSet.FIELD_EDGE_GRAPH_IDS)
      .field(TableBaseLayout.DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
      .as(GVETableSet.FIELD_EDGE_PROPERTIES);

    Table edgesTable = TableUtils.registerDataSet(getConfig().getTableEnvironment(), edges,
      edgeFieldsBuilder.buildSeq());

    return tableSetFactory.fromTables(verticesTable, edgesTable, graphsTable);
  }
}
