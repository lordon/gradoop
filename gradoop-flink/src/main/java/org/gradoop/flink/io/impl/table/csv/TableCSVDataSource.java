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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.api.table.TableDataSource;
import org.gradoop.flink.io.impl.table.csv.functions.ParseGradoopId;
import org.gradoop.flink.io.impl.table.csv.functions.ParseGradoopIdSet;
import org.gradoop.flink.io.impl.table.csv.functions.ParseProperties;
import org.gradoop.flink.io.impl.table.csv.functions.ParsePropertyValue;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollectionFactory;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraphFactory;
import org.gradoop.flink.model.impl.layouts.table.TableSet;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
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
  public TableLogicalGraph getLogicalGraph() throws Exception {
    TableLogicalGraphFactory factory = this.config.getTableLogicalGraphFactory();
    TableSet tableSet = registerTables(readSchema());
    return factory.fromTableSet(tableSet);
  }

  @Override
  public TableGraphCollection getGraphCollection() throws Exception {
    TableGraphCollectionFactory factory = this.config.getTableGraphCollectionFactory();
    TableSet tableSet = registerTables(readSchema());
    return factory.fromTableSet(tableSet);
  }

  /**
   * Reads table set schema from files.
   *
   * @return table set
   * @throws Exception
   */
  private TableSetSchema readSchema() throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    Charset charset = Charset.forName("UTF-8");

    HashMap<String, TableSchema> schemaMap = new HashMap<>();

    Path tablesPath = new Path(new StringBuilder()
      .append(this.csvRoot)
      .append(SCHEMA_DIR)
      .append(FILE_NAME_TABLES)
      .append(CSV_FILE_SUFFIX).toString());

    if (!fs.exists(tablesPath)) {
      throw new FileNotFoundException(tablesPath.getName());
    } else {
      try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(fs.open(tablesPath), charset))) {
        String tableName;
        while ((tableName = reader.readLine()) != null) {
          TableSchema schema = readTableSchema(tableName, fs, charset);
          schemaMap.put(tableName, schema);
        }
      }
    }

    return new TableSetSchema(schemaMap);
  }

  /**
   * Reads schema of specified table.
   *
   * @param tableName name of table to read schema of
   * @param fs file system instance
   * @param charset charset
   * @return table schema of specified table
   * @throws Exception
   */
  private TableSchema readTableSchema(String tableName, FileSystem fs, Charset charset)
    throws Exception {

    TableSchemaBuilder schemaBuilder = new TableSchemaBuilder();

    Path tablePath = new Path(new StringBuilder()
      .append(this.csvRoot)
      .append(SCHEMA_DIR)
      .append(tableName)
      .append(CSV_FILE_SUFFIX).toString());

    if (!fs.exists(tablePath)) {
      throw new FileNotFoundException(tablePath.getName());
    } else {
      try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(fs.open(tablePath), charset))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] field = line.split(CSV_FIELD_DELIMITER);

          if (field.length != 2) {
            throw new RuntimeException("Bad schema line " + line);
          }

          schemaBuilder.field(field[0], TypeInformation.of(Class.forName(field[1])));
        }
      }
    }

    return schemaBuilder.build();
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

      // Each field is expected to be a string
      for (String fieldName : tableSchema.getColumnNames()) {
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
