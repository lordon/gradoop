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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.api.table.TableDataSink;
import org.gradoop.flink.model.api.layouts.table.BaseTableSet;
import org.gradoop.flink.model.api.layouts.table.TableGraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayout;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.io.impl.table.csv.functions.GradoopIdSetToBase64;
import org.gradoop.flink.io.impl.table.csv.functions.GradoopIdToString;
import org.gradoop.flink.io.impl.table.csv.functions.PropertiesToBase64JsonString;
import org.gradoop.flink.io.impl.table.csv.functions.PropertyValueToBase64;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Data sink for {@link TableLogicalGraph} and {@link TableGraphCollection} writing separate
 * CSV files for each table of current layout.
 * Based on Flink's {@link CsvTableSink}
 */
public class TableCSVDataSink extends TableCSVBase implements TableDataSink {

  /**
   * Mapping of data types to composing scalar functions which produce strings
   */
  private static ImmutableMap<Class, ScalarFunction> TYPE_COMPOSER =
    ImmutableMap.<Class, ScalarFunction>builder()
      .put(PropertyValue.class, new PropertyValueToBase64())
      .put(GradoopId.class, new GradoopIdToString())
      .put(Properties.class, new PropertiesToBase64JsonString())
      .put(GradoopIdSet.class, new GradoopIdSetToBase64())
      .build();

  /**
   * Constructor
   * @param csvPath directory path to write files to
   * @param config gradoop flink config
   */
  public TableCSVDataSink(String csvPath, GradoopFlinkConfig config) {
    super(csvPath, config);
  }

  @Override
  public void write(TableLogicalGraph logicalGraph) throws IOException {
    write(logicalGraph.getLayout());
  }

  @Override
  public void write(TableGraphCollection graphCollection) throws IOException {
    write(graphCollection.getLayout());
  }

  @Override
  public void write(TableLogicalGraph logicalGraph, boolean overwrite) throws IOException {
    write(logicalGraph.getLayout(), overwrite);
  }

  @Override
  public void write(TableGraphCollection graphCollection, boolean overwrite) throws IOException {
    write(graphCollection.getLayout(), overwrite);
  }

  @Override
  public void write(TableLogicalGraphLayout logicalGraphLayout) throws IOException {
    writeTableSet(logicalGraphLayout.getTableSet(), false);
  }

  @Override
  public void write(TableGraphCollectionLayout graphCollectionLayout) throws IOException {
    writeTableSet(graphCollectionLayout.getTableSet(), false);
  }

  @Override
  public void write(TableLogicalGraphLayout logicalGraphLayout, boolean overwrite) throws
    IOException {
    writeTableSet(logicalGraphLayout.getTableSet(), overwrite);
  }

  @Override
  public void write(TableGraphCollectionLayout graphCollectionLayout, boolean overwrite) throws
    IOException {
    writeTableSet(graphCollectionLayout.getTableSet(), overwrite);
  }

  /**
   * Writes a CSV file for each table of given table set. Each field is stored as string.
   * Table set needs to match schema of current layout!
   * For table set and each table a simple schema file is written, too.
   *
   * @param tableSet table set to write
   * @param overwrite true, if existing files should be overwritten
   */
  private void writeTableSet(BaseTableSet tableSet, boolean overwrite) throws IOException {
    BatchTableEnvironment env = config.getTableEnvironment();

    FileSystem.WriteMode writeMode =
      overwrite ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE;

    TableSetSchema schema = tableSet.getSchema();

    // Write set of table names
    Collection<Tuple1> tableNames = schema.getTableNames().stream()
      .map(tableName -> Tuple1.of(tableName))
      .collect(Collectors.toSet());

    config.getExecutionEnvironment().fromCollection(tableNames).writeAsCsv(
      new StringBuilder()
        .append(csvRoot)
        .append(SCHEMA_DIR)
        .append(FILE_NAME_TABLES)
        .append(CSV_FILE_SUFFIX)
        .toString(), writeMode);

    // For each table in table set
    for (Map.Entry<String, Table> table : tableSet.entrySet()) {
      String tableName = table.getKey();
      TableSchema tableSchema = schema.getTable(tableName);

      // Instantiate table sink
      String path =
        new StringBuilder().append(csvRoot).append(tableName).append(CSV_FILE_SUFFIX).toString();
      CsvTableSink tableSink = new CsvTableSink(path, CSV_FIELD_DELIMITER,
        config.getExecutionEnvironment().getParallelism(), writeMode);

      String[] fieldNames = tableSchema.getFieldNames();
      TypeInformation[] fieldTypes = new TypeInformation[fieldNames.length];
      Arrays.fill(fieldTypes, Types.STRING());
      // Name of output table must be unique
      String outputTableName =
        new StringBuilder().append(tableName).append(GradoopId.get()).toString();
      env.registerTableSink(
        outputTableName,
        fieldNames,
        fieldTypes,
        tableSink
      );

      // Write table
      table.getValue()
        .select(buildProjectExpressions(tableSchema, TYPE_COMPOSER))
        .insertInto(outputTableName);

      // Write table schema
      Collection<Tuple2<String, String>> fieldTypeTuples = new ArrayList<>();
      for (int i = 0; i < tableSchema.getFieldCount(); i++) {
        fieldTypeTuples.add(Tuple2.of(
          tableSchema.getFieldName(i).get(),
          tableSchema.getFieldType(i).get().getTypeClass().getTypeName()
        ));
      }

      config.getExecutionEnvironment().fromCollection(fieldTypeTuples).writeAsCsv(
        new StringBuilder()
          .append(csvRoot)
          .append(SCHEMA_DIR)
          .append(tableName)
          .append(CSV_FILE_SUFFIX)
          .toString(), writeMode);
    }
  }
}
