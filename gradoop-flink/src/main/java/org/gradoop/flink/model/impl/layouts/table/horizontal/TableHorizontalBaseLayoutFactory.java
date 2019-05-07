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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.layouts.table.TableBaseLayoutFactory;
import org.gradoop.flink.model.impl.functions.epgm.GraphElementExpander;
import org.gradoop.flink.model.impl.functions.tuple.Project3To0And2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of3;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.model.impl.layouts.table.common.functions.dataset.ByPropertyKey;
import org.gradoop.flink.model.impl.layouts.table.common.functions.dataset.EdgeWithLabel;
import org.gradoop.flink.model.impl.layouts.table.common.functions.dataset.ElementIdWithLabel;
import org.gradoop.flink.model.impl.layouts.table.common.functions.dataset.PairElementIdWithPropertyKeysAndValues;
import org.gradoop.flink.model.impl.layouts.table.common.functions.dataset.SwitchPairAndExtractElementId;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for all layout factories of horizontal layout
 */
public class TableHorizontalBaseLayoutFactory extends BaseFactory
  implements TableBaseLayoutFactory<HorizontalTableSet> {

  @Override
  public TableSetSchema getSchema() {
    return new HorizontalTableSet().getSchema();
  }

  @Override
  public HorizontalTableSet tableSetfromDataSets(DataSet<GraphHead> graphHeads,
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

    DataSet<Tuple3<GradoopId, String, PropertyValue>> propertyValues = vertices
      .flatMap(new PairElementIdWithPropertyKeysAndValues())
      .union(edges.flatMap(new PairElementIdWithPropertyKeysAndValues()))
      .union(graphHeads.flatMap(new PairElementIdWithPropertyKeysAndValues()));

    List<PropertyTable> propertyTables = new ArrayList<>();

    List<String> propertyKeys = new ArrayList<>();

    try {
      propertyKeys = propertyValues
        .map(new Value1Of3<>())
        .distinct()
        .collect();
    } catch (Exception e) {
      e.printStackTrace();
    }

    for (String propertyKey : propertyKeys) {
      TableSchema tableSchema = PropertyTable.buildPropertyTableSchema(propertyKey);

      DataSet<Tuple2<GradoopId, PropertyValue>> propertyValuesDataSet = propertyValues
        .filter(new ByPropertyKey(propertyKey))
        .map(new Project3To0And2<>());

      Table propertyValuesTable = dataSetToTable(propertyValuesDataSet,
        String.join(",", tableSchema.getColumnNames()));

      propertyTables.add(new PropertyTable(propertyKey, propertyValuesTable));
    }

    Table verticesTable = dataSetToTable(verticesDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(HorizontalTableSet.TABLE_VERTICES));
    Table edgesTable = dataSetToTable(edgesDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(HorizontalTableSet.TABLE_EDGES));
    Table graphsTable = dataSetToTable(graphsDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(HorizontalTableSet.TABLE_GRAPHS));
    Table verticesGraphsTable = dataSetToTable(verticesGraphsDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(HorizontalTableSet.TABLE_VERTICES_GRAPHS));
    Table edgesGraphsTable = dataSetToTable(edgesGraphsDataSet, getSchema()
      .commaSeparatedFieldNamesForTable(HorizontalTableSet.TABLE_EDGES_GRAPHS));

    return new HorizontalTableSetFactory(getConfig()).fromTables(
      verticesTable,
      edgesTable,
      graphsTable,
      verticesGraphsTable,
      edgesGraphsTable,
      propertyTables
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
