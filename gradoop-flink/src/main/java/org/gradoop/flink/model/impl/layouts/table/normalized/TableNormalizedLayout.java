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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.expressions.RowConstructor;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.layouts.table.TableLayout;
import org.gradoop.flink.model.impl.layouts.table.TableBaseLayout;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate.CollectGradoopIdSet;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate.CollectProperties;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.EmptyGradoopIdSetIfNull;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.EmptyPropertiesIfNull;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for all normalized layouts.
 */
public class TableNormalizedLayout
  extends TableBaseLayout<NormalizedTableSet, NormalizedTableSetFactory> implements TableLayout {

  /**
   * Constructor
   *
   * @param tableSet normalized table set
   * @param config gradoop configuration
   */
  TableNormalizedLayout(NormalizedTableSet tableSet, GradoopFlinkConfig config) {
    super(tableSet, config);
    this.tableSetFactory = new NormalizedTableSetFactory(config);
  }

  @Override
  public DataSet<Vertex> toVertexDataSet() {
    ExpressionBuilder builder = new ExpressionBuilder();

    // TODO Why does using createUniqueAttributeName not work here?
    String vIdTmpFieldName1 = "toVertexDataSetTmp1";
    String vIdTmpFieldName2 = "toVertexDataSetTmp2";
    String propertyValueTmpFieldName = "toVertexDataSetTmp3";

    Table verticesWithProperties = tableSet.getVertices()
      .select(new ExpressionSeqBuilder()
        .field(NormalizedTableSet.FIELD_VERTEX_ID)
        .as(vIdTmpFieldName1).buildSeq())
      .join(tableSet.getVertexPropertyValues(), builder
        .field(vIdTmpFieldName1)
        .equalTo(NormalizedTableSet.FIELD_PROPERTY_VERTEX_ID)
        .toExpression())
      .select(new ExpressionSeqBuilder()
        .field(vIdTmpFieldName1)
        .expression(new RowConstructor(new ExpressionSeqBuilder()
            .resolvedField(NormalizedTableSet.FIELD_VERTEX_PROPERTY_NAME,
              Types.STRING())
            .resolvedField(NormalizedTableSet.FIELD_VERTEX_PROPERTY_VALUE,
              TypeInformation.of(PropertyValue.class))
            .buildSeq()
          )
        )
        .as(propertyValueTmpFieldName)
        .buildSeq()
      )
      .groupBy(vIdTmpFieldName1)
      .select(new ExpressionSeqBuilder()
        .field(vIdTmpFieldName1)
        .aggFunctionCall(new CollectProperties(), propertyValueTmpFieldName)
        .as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .buildSeq()
      );

    Table verticesWithGraphIds = tableSet.getVertices()
        .select(new ExpressionSeqBuilder()
          .field(NormalizedTableSet.FIELD_VERTEX_ID)
          .as(vIdTmpFieldName2).buildSeq())
        .join(tableSet.getVerticesGraphs(), builder
          .field(vIdTmpFieldName2)
          .equalTo(NormalizedTableSet.FIELD_GRAPH_VERTEX_ID)
          .toExpression())
        .groupBy(vIdTmpFieldName2)
        .select(new ExpressionSeqBuilder()
          .field(vIdTmpFieldName2)
          .aggFunctionCall(new CollectGradoopIdSet(), NormalizedTableSet.FIELD_VERTEX_GRAPH_ID)
          .as(DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD)
          .buildSeq()
        );

    Table vertices = tableSet.getVertices()
      .leftOuterJoin(verticesWithProperties, builder
        .field(NormalizedTableSet.FIELD_VERTEX_ID).equalTo(vIdTmpFieldName1).toExpression())
      .leftOuterJoin(verticesWithGraphIds, builder
        .field(NormalizedTableSet.FIELD_VERTEX_ID).equalTo(vIdTmpFieldName2).toExpression())
      .select(new ExpressionSeqBuilder()
        .field(NormalizedTableSet.FIELD_VERTEX_ID).as(DATASET_EPGM_ELEMENT_ID_FIELD)
        .field(NormalizedTableSet.FIELD_VERTEX_LABEL).as(DATASET_EPGM_ELEMENT_LABEL_FIELD)
        .scalarFunctionCall(new EmptyPropertiesIfNull(),
          DATASET_EPGM_ELEMENT_PROPERTIES_FIELD).as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .scalarFunctionCall(new EmptyGradoopIdSetIfNull(),
          DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD).as(DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD)
        .buildSeq()
      );

    return config.getTableEnvironment().toDataSet(vertices, Vertex.class);
  }

  @Override
  public DataSet<Edge> toEdgeDataSet() {
    ExpressionBuilder builder = new ExpressionBuilder();

    // TODO Why does using createUniqueAttributeName not work here?
    String eIdTmpFieldName1 = "toEdgeDataSetTmp1";
    String eIdTmpFieldName2 = "toEdgeDataSetTmp2";
    String propertyValueTmpFieldName = "toEdgeDataSetTmp3";

    Table edgesWithProperties = tableSet.getEdges()
      .select(new ExpressionSeqBuilder()
        .field(NormalizedTableSet.FIELD_EDGE_ID)
        .as(eIdTmpFieldName1).buildSeq())
      .join(tableSet.getEdgePropertyValues(), builder
        .field(eIdTmpFieldName1).equalTo(NormalizedTableSet.FIELD_PROPERTY_EDGE_ID).toExpression())
      .select(new ExpressionSeqBuilder()
        .field(eIdTmpFieldName1)
        .expression(new RowConstructor(new ExpressionSeqBuilder()
          .resolvedField(NormalizedTableSet.FIELD_EDGE_PROPERTY_NAME,
            Types.STRING())
          .resolvedField(NormalizedTableSet.FIELD_EDGE_PROPERTY_VALUE,
            TypeInformation.of(PropertyValue.class))
          .buildSeq()
        ))
        .as(propertyValueTmpFieldName)
        .buildSeq()
      )
      .groupBy(eIdTmpFieldName1)
      .select(new ExpressionSeqBuilder()
        .field(eIdTmpFieldName1)
        .aggFunctionCall(new CollectProperties(), propertyValueTmpFieldName)
        .as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .buildSeq()
      );

    Table edgesWithGraphIds = tableSet.getEdges()
      .select(new ExpressionSeqBuilder()
        .field(NormalizedTableSet.FIELD_EDGE_ID)
        .as(eIdTmpFieldName2).buildSeq())
      .join(tableSet.getEdgesGraphs(), builder
        .field(eIdTmpFieldName2).equalTo(NormalizedTableSet.FIELD_GRAPH_EDGE_ID).toExpression())
      .groupBy(eIdTmpFieldName2)
      .select(new ExpressionSeqBuilder()
        .field(eIdTmpFieldName2)
        .aggFunctionCall(new CollectGradoopIdSet(), NormalizedTableSet.FIELD_EDGE_GRAPH_ID)
        .as(DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD)
        .buildSeq()
      );

    Table edges = tableSet.getEdges()
      .leftOuterJoin(edgesWithProperties, builder
        .field(NormalizedTableSet.FIELD_EDGE_ID).equalTo(eIdTmpFieldName1).toExpression())
      .leftOuterJoin(edgesWithGraphIds, builder
        .field(NormalizedTableSet.FIELD_EDGE_ID).equalTo(eIdTmpFieldName2).toExpression())
      .select(new ExpressionSeqBuilder()
        .field(NormalizedTableSet.FIELD_EDGE_ID).as(DATASET_EPGM_ELEMENT_ID_FIELD)
        .field(NormalizedTableSet.FIELD_EDGE_LABEL).as(DATASET_EPGM_ELEMENT_LABEL_FIELD)
        .field(NormalizedTableSet.FIELD_TAIL_ID).as(DATASET_EPGM_EDGE_SOURCE_ID_FIELD)
        .field(NormalizedTableSet.FIELD_HEAD_ID).as(DATASET_EPGM_EDGE_TARGET_ID_FIELD)
        .scalarFunctionCall(new EmptyPropertiesIfNull(),
          DATASET_EPGM_ELEMENT_PROPERTIES_FIELD).as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .scalarFunctionCall(new EmptyGradoopIdSetIfNull(),
          DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD).as(DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD)
        .buildSeq()
      );

    return config.getTableEnvironment().toDataSet(edges, TypeInformation.of(Edge.class));
  }

  @Override
  public DataSet<GraphHead> toGraphHeadDataSet() {
    ExpressionBuilder builder = new ExpressionBuilder();

    // TODO Why does using createUniqueAttributeName not work here?
    String gIdTmpFieldName = "toGraphHeadDataSetTmp1";
    String propertyValueTmpFieldName = "toGraphHeadDataSetTmp2";

    Table graphsWithProperties = tableSet.getGraphs()
      .select(new ExpressionSeqBuilder()
        .field(NormalizedTableSet.FIELD_GRAPH_ID)
        .as(gIdTmpFieldName).buildSeq())
      .join(tableSet.getGraphPropertyValues(), builder
        .field(gIdTmpFieldName).equalTo(NormalizedTableSet.FIELD_PROPERTY_GRAPH_ID).toExpression())
      .select(new ExpressionSeqBuilder()
        .field(gIdTmpFieldName)
        .expression(new RowConstructor(new ExpressionSeqBuilder()
          .resolvedField(NormalizedTableSet.FIELD_GRAPH_PROPERTY_NAME,
            Types.STRING())
          .resolvedField(NormalizedTableSet.FIELD_GRAPH_PROPERTY_VALUE,
            TypeInformation.of(PropertyValue.class))
          .buildSeq()
        ))
        .as(propertyValueTmpFieldName)
        .buildSeq()
      )
      .groupBy(gIdTmpFieldName)
      .select(new ExpressionSeqBuilder()
        .field(gIdTmpFieldName)
        .aggFunctionCall(new CollectProperties(), propertyValueTmpFieldName)
        .as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .buildSeq()
      );

    Table graphs = tableSet.getGraphs()
      .leftOuterJoin(graphsWithProperties, builder
        .field(NormalizedTableSet.FIELD_GRAPH_ID).equalTo(gIdTmpFieldName).toExpression())
      .select(new ExpressionSeqBuilder()
        .field(NormalizedTableSet.FIELD_GRAPH_ID).as(DATASET_EPGM_ELEMENT_ID_FIELD)
        .field(NormalizedTableSet.FIELD_GRAPH_LABEL).as(DATASET_EPGM_ELEMENT_LABEL_FIELD)
        .scalarFunctionCall(new EmptyPropertiesIfNull(),
          DATASET_EPGM_ELEMENT_PROPERTIES_FIELD).as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .buildSeq()
      );

    return config.getTableEnvironment().toDataSet(graphs, TypeInformation.of(GraphHead.class));
  }
}
