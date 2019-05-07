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
package org.gradoop.benchmark.table;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.api.table.TableDataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.table.csv.TableCSVDataSink;
import org.gradoop.flink.io.impl.table.csv.TableCSVDataSource;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A dedicated program for benchmark of Difference operator. Operator can be executed in dataset
 * implementation or in one of the implementation based on Table-API.
 */
public class TableDifferenceBenchmark extends TableSetOperatorBaseBenchmark {

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception in case of Error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TableDifferenceBenchmark.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    // read cmd arguments
    readCMDArguments(cmd);

    // create gradoop config
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    if (USE_TABLE_LAYOUT) {
      setTableFactories(TABLE_LAYOUT, conf);

      TableDataSource firstSource = new TableCSVDataSource(FIRST_INPUT_PATH, conf);
      TableDataSource secondSource = new TableCSVDataSource(SECOND_INPUT_PATH, conf);

      firstSource.getGraphCollection().difference(secondSource.getGraphCollection())
        .writeTo(new TableCSVDataSink(OUTPUT_PATH, conf));
    } else {
      DataSource firstSource = new CSVDataSource(FIRST_INPUT_PATH, conf);
      DataSource secondSource = new CSVDataSource(SECOND_INPUT_PATH, conf);

      firstSource.getGraphCollection().difference(secondSource.getGraphCollection())
        .writeTo(new CSVDataSink(OUTPUT_PATH, conf));
    }

    // execute and write job statistics
    env.execute();
    writeCSV(env);
  }

}
