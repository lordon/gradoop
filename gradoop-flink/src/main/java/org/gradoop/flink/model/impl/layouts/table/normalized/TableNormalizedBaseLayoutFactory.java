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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.layouts.table.TableBaseLayoutFactory;
import org.gradoop.flink.model.impl.functions.epgm.GraphElementExpander;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.model.impl.layouts.table.common.functions.dataset.EdgeWithLabel;
import org.gradoop.flink.model.impl.layouts.table.common.functions.dataset.ElementIdWithLabel;
import org.gradoop.flink.model.impl.layouts.table.common.functions.dataset.PairElementIdWithPropertyKeysAndValues;
import org.gradoop.flink.model.impl.layouts.table.common.functions.dataset.SwitchPairAndExtractElementId;

/**
 * Base class for all layout factories of normalized layout
 */
public abstract class TableNormalizedBaseLayoutFactory extends BaseFactory
  implements TableBaseLayoutFactory<NormalizedTableSet> {

  @Override
  public TableSetSchema getSchema() {
    return NormalizedTableSet.SCHEMA;
  }

  @Override
  public NormalizedTableSet tableSetfromDataSets(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices, DataSet<Edge> edges) {
    DataSet<Tuple2<GradoopId, String>> verticesDataSet = vertices
      .map(new ElementIdWithLabel<>());

    DataSet<Tuple4<GradoopId, GradoopId, GradoopId, String>> edgesDataSet = edges
      .map(new EdgeWithLabel());

    DataSet<Tuple2<GradoopId, String>> graphsDataSet = graphHeads
      .map(new ElementIdWithLabel<>());

    DataSet<Tuple2<GradoopId, GradoopId>> verticesGraphsDataSet = vertices
      .flatMap(new GraphElementExpander<>())
      .map(new SwitchPairAndExtractElementId<>());

    DataSet<Tuple2<GradoopId, GradoopId>> edgesGraphsDataSet = edges
      .flatMap(new GraphElementExpander<>())
      .map(new SwitchPairAndExtractElementId<>());

    DataSet<Tuple3<GradoopId, String, PropertyValue>> vertexPropertyValuesDataSet = vertices
      .flatMap(new PairElementIdWithPropertyKeysAndValues<>());

    DataSet<Tuple3<GradoopId, String, PropertyValue>> edgePropertyValuesDataSet = edges
      .flatMap(new PairElementIdWithPropertyKeysAndValues<>());

    DataSet<Tuple3<GradoopId, String, PropertyValue>> graphPropertyValuesDataSet = graphHeads
      .flatMap(new PairElementIdWithPropertyKeysAndValues<>());

    Table verticesTable = dataSetToTable(verticesDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(NormalizedTableSet.TABLE_VERTICES));
    Table edgesTable = dataSetToTable(edgesDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(NormalizedTableSet.TABLE_EDGES));
    Table graphsTable = dataSetToTable(graphsDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(NormalizedTableSet.TABLE_GRAPHS));
    Table verticesGraphsTable = dataSetToTable(verticesGraphsDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(NormalizedTableSet.TABLE_VERTICES_GRAPHS));
    Table edgesGraphsTable = dataSetToTable(edgesGraphsDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(NormalizedTableSet.TABLE_EDGES_GRAPHS));
    Table vertexPropertyValuesTable = dataSetToTable(vertexPropertyValuesDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(NormalizedTableSet.TABLE_VERTEX_PROPERTY_VALUES));
    Table edgePropertyValuesTable = dataSetToTable(edgePropertyValuesDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(NormalizedTableSet.TABLE_EDGE_PROPERTY_VALUES));
    Table graphPropertyValuesTable = dataSetToTable(graphPropertyValuesDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(NormalizedTableSet.TABLE_GRAPH_PROPERTY_VALUES));

    return new NormalizedTableSetFactory(getConfig()).fromTables(
      verticesTable,
      edgesTable,
      graphsTable,
      verticesGraphsTable,
      edgesGraphsTable,
      vertexPropertyValuesTable,
      edgePropertyValuesTable,
      graphPropertyValuesTable
    );
  }

  /**
   * Converts given data set to table with given field names
   *
   * @param dataSet data set
   * @param fields field names
   * @return table
   */
  private Table dataSetToTable(DataSet<?> dataSet, String fields) {
    return this.getConfig().getTableEnvironment().fromDataSet(dataSet, fields);
  }
}
