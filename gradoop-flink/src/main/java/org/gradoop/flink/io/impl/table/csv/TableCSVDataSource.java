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
package org.gradoop.flink.io.impl.table.csv;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.api.table.TableDataSource;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollectionFactory;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraphFactory;
import org.gradoop.flink.model.impl.layouts.table.TableSchema;
import org.gradoop.flink.model.impl.layouts.table.TableSet;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.io.impl.table.csv.functions.ParseGradoopId;
import org.gradoop.flink.io.impl.table.csv.functions.ParseGradoopIdSet;
import org.gradoop.flink.io.impl.table.csv.functions.ParseProperties;
import org.gradoop.flink.io.impl.table.csv.functions.ParsePropertyValue;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Map;

/**
 * Data source for {@link TableLogicalGraph} and {@link TableGraphCollection} reading separate
 * CSV files for each table of current layout.
 * Based on Flink's {@link CsvTableSource}
 */
public class TableCSVDataSource extends TableCSVBase implements TableDataSource {

  /**
   * Mapping of data types to scalar functions which parse strings to corresponding data type
   */
  private static ImmutableMap<Class, ScalarFunction> TYPE_PARSER =
    ImmutableMap.<Class, ScalarFunction>builder()
      .put(PropertyValue.class, new ParsePropertyValue())
      .put(GradoopId.class, new ParseGradoopId())
      .put(Properties.class, new ParseProperties())
      .put(GradoopIdSet.class, new ParseGradoopIdSet())
      .build();

  /**
   * Constructor
   * @param csvPath directory path to read files frin
   * @param config gradoop flink config
   */
  public TableCSVDataSource(String csvPath, GradoopFlinkConfig config) {
    super(csvPath, config);
  }

  @Override
  public TableLogicalGraph getLogicalGraph() throws IOException {
    TableLogicalGraphFactory factory = this.config.getTableLogicalGraphFactory();
    TableSet tableSet = registerTables(factory.getSchema());
    return factory.fromTableSet(tableSet);
  }

  @Override
  public TableGraphCollection getGraphCollection() throws IOException {
    TableGraphCollectionFactory factory = this.config.getTableGraphCollectionFactory();
    TableSet tableSet = registerTables(factory.getSchema());
    return factory.fromTableSet(tableSet);
  }

  /**
   * Reads a CSV file for each table defined in given schema. Fields get loaded as strings
   * and parsed to complex data types afterwards. Loaded tables get bundled as {@link TableSet}.
   *
   * @param schema schema (map of table name -> table schema)
   * @return new table set referencing newly loaded tables
   */
  private TableSet registerTables(TableSetSchema schema) {
    BatchTableEnvironment env = config.getTableEnvironment();
    TableSet tableSet = new TableSet();

    // For each table in schema
    for (Map.Entry<String, TableSchema> table : schema.getTables()) {
      String tableName = table.getKey();
      TableSchema tableSchema = schema.getTable(tableName);

      String path = new StringBuilder()
        .append(this.csvRoot).append(tableName).append(CSV_FILE_SUFFIX).toString();

      CsvTableSource.Builder sourceBuilder = CsvTableSource.builder().path(path);

      // Each column is expected to be a string
      for (String fieldName : tableSchema.getFieldNames()) {
        sourceBuilder.field(fieldName, Types.STRING());
      }

      // Parse strings to respective data types
      Table newTable = env.fromTableSource(sourceBuilder.build())
        .select(buildProjectExpressions(tableSchema, TYPE_PARSER));

      tableSet.put(tableName, newTable);
    }

    return tableSet;
  }

}
