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
package org.gradoop.flink.model.impl.layouts.table.horizontal;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.RowConstructor;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.layouts.table.TableLayout;
import org.gradoop.flink.model.impl.layouts.table.TableBaseLayout;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate.CollectGradoopIdSet;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.EmptyGradoopIdSetIfNull;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.EmptyProperties;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.EmptyPropertiesIfNull;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.ToProperties;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionBuilder;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

/**
 * Base class for all horizontal layouts.
 */
public class TableHorizontalLayout extends
  TableBaseLayout<HorizontalTableSet, HorizontalTableSetFactory> implements TableLayout {

  /**
   * Constructor
   *
   * @param tableSet horizontal table set
   * @param config gradoop configuration
   */
  public TableHorizontalLayout(HorizontalTableSet tableSet, GradoopFlinkConfig config) {
    super(tableSet, config);
    this.tableSetFactory = new HorizontalTableSetFactory(config);
  }

  @Override
  public DataSet<Vertex> toVertexDataSet() {
    ExpressionBuilder builder = new ExpressionBuilder();

    // TODO Why does using createUniqueAttributeName not work here?
    String vIdTmpFieldName1 = "toVertexDataSetTmp1";
    String vIdTmpFieldName2 = "toVertexDataSetTmp2";

    Table verticesWithProperties = joinProperties(tableSet.getVertices(),
      HorizontalTableSet.FIELD_VERTEX_ID, vIdTmpFieldName1);

    Table verticesWithGraphIds = tableSet.getVertices()
      .select(new ExpressionSeqBuilder()
        .field(HorizontalTableSet.FIELD_VERTEX_ID)
        .as(vIdTmpFieldName2).buildSeq())
      .join(tableSet.getVerticesGraphs(), builder
        .field(vIdTmpFieldName2)
        .equalTo(HorizontalTableSet.FIELD_GRAPH_VERTEX_ID)
        .toExpression())
      .groupBy(vIdTmpFieldName2)
      .select(new ExpressionSeqBuilder()
        .field(vIdTmpFieldName2)
        .aggFunctionCall(new CollectGradoopIdSet(), HorizontalTableSet.FIELD_VERTEX_GRAPH_ID)
        .as(DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD)
        .buildSeq()
      );

    Table vertices = tableSet.getVertices()
      .leftOuterJoin(verticesWithProperties, builder
        .field(HorizontalTableSet.FIELD_VERTEX_ID).equalTo(vIdTmpFieldName1).toExpression())
      .leftOuterJoin(verticesWithGraphIds, builder
        .field(HorizontalTableSet.FIELD_VERTEX_ID).equalTo(vIdTmpFieldName2).toExpression())
      .select(new ExpressionSeqBuilder()
        .field(HorizontalTableSet.FIELD_VERTEX_ID).as(DATASET_EPGM_ELEMENT_ID_FIELD)
        .field(HorizontalTableSet.FIELD_VERTEX_LABEL).as(DATASET_EPGM_ELEMENT_LABEL_FIELD)
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

    Table edgesWithProperties = joinProperties(tableSet.getEdges(),
      HorizontalTableSet.FIELD_EDGE_ID, eIdTmpFieldName1);

    Table edgesWithGraphIds = tableSet.getEdges()
      .select(new ExpressionSeqBuilder()
        .field(HorizontalTableSet.FIELD_EDGE_ID)
        .as(eIdTmpFieldName2).buildSeq())
      .join(tableSet.getEdgesGraphs(), builder
        .field(eIdTmpFieldName2).equalTo(HorizontalTableSet.FIELD_GRAPH_EDGE_ID).toExpression())
      .groupBy(eIdTmpFieldName2)
      .select(new ExpressionSeqBuilder()
        .field(eIdTmpFieldName2)
        .aggFunctionCall(new CollectGradoopIdSet(), HorizontalTableSet.FIELD_EDGE_GRAPH_ID)
        .as(DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD)
        .buildSeq()
      );

    Table edges = tableSet.getEdges()
      .leftOuterJoin(edgesWithProperties, builder
        .field(HorizontalTableSet.FIELD_EDGE_ID).equalTo(eIdTmpFieldName1).toExpression())
      .leftOuterJoin(edgesWithGraphIds, builder
        .field(HorizontalTableSet.FIELD_EDGE_ID).equalTo(eIdTmpFieldName2).toExpression())
      .select(new ExpressionSeqBuilder()
        .field(HorizontalTableSet.FIELD_EDGE_ID).as(DATASET_EPGM_ELEMENT_ID_FIELD)
        .field(HorizontalTableSet.FIELD_EDGE_LABEL).as(DATASET_EPGM_ELEMENT_LABEL_FIELD)
        .field(HorizontalTableSet.FIELD_TAIL_ID).as(DATASET_EPGM_EDGE_SOURCE_ID_FIELD)
        .field(HorizontalTableSet.FIELD_HEAD_ID).as(DATASET_EPGM_EDGE_TARGET_ID_FIELD)
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

    Table graphsWithProperties = joinProperties(tableSet.getGraphs(),
      HorizontalTableSet.FIELD_GRAPH_ID, gIdTmpFieldName);

    Table graphs = tableSet.getGraphs()
      .leftOuterJoin(graphsWithProperties, builder
        .field(HorizontalTableSet.FIELD_GRAPH_ID).equalTo(gIdTmpFieldName).toExpression())
      .select(new ExpressionSeqBuilder()
        .field(HorizontalTableSet.FIELD_GRAPH_ID).as(DATASET_EPGM_ELEMENT_ID_FIELD)
        .field(HorizontalTableSet.FIELD_GRAPH_LABEL).as(DATASET_EPGM_ELEMENT_LABEL_FIELD)
        .scalarFunctionCall(new EmptyPropertiesIfNull(),
          DATASET_EPGM_ELEMENT_PROPERTIES_FIELD).as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .buildSeq()
      );

    return config.getTableEnvironment().toDataSet(graphs, TypeInformation.of(GraphHead.class));
  }

  /**
   * Joins property tables of horizontal layout to elements (graphs, vertices or edges).
   *
   * @param tableToJoin table to join with properties
   * @param elementIdFieldName name of element id field
   * @param elementIdAliasFieldName alias name the element id gets projected to
   * @return element table with joined properties
   */
  private Table joinProperties(Table tableToJoin, String elementIdFieldName,
    String elementIdAliasFieldName) {
    List<PropertyTable> propertyTables = getTableSet().getPropertyTables();

    if (propertyTables.size() > 0) {
      ExpressionBuilder builder = new ExpressionBuilder();
      ExpressionSeqBuilder rowExpressionsBuilder = new ExpressionSeqBuilder();

      for (PropertyTable propertyTable : propertyTables) {
        tableToJoin = tableToJoin
          .leftOuterJoin(propertyTable.getTable(), builder
            .field(elementIdFieldName)
            .equalTo(propertyTable.getElementIdFieldName()).toExpression());

        rowExpressionsBuilder
          .expression(new Literal(propertyTable.getPropertyKey(), Types.STRING()))
          .resolvedField(propertyTable.getPropertyValueFieldName(),
            TypeInformation.of(PropertyValue.class));
      }

      return tableToJoin.select(new ExpressionSeqBuilder()
        .field(elementIdFieldName).as(elementIdAliasFieldName)
        .scalarFunctionCall(new ToProperties(), new Expression[]{
          new RowConstructor(rowExpressionsBuilder.buildSeq()) })
        .as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .buildSeq()
      );
    } else {
      // Since flink does not support empty rows yet
      return tableToJoin.select(new ExpressionSeqBuilder()
        .field(elementIdFieldName).as(elementIdAliasFieldName)
        .scalarFunctionCall(new EmptyProperties())
        .as(DATASET_EPGM_ELEMENT_PROPERTIES_FIELD)
        .buildSeq());
    }
  }

}
