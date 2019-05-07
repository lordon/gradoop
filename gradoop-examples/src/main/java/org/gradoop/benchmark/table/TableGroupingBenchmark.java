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
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.table.csv.TableCSVDataSink;
import org.gradoop.flink.io.impl.table.csv.TableCSVDataSource;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinProperty;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for table based graph grouping benchmark. Grouping configurations are fix.
 */
public class TableGroupingBenchmark extends AbstractRunner {

  /**
   * Option to declare path to indexed input graph
   */
  private static final String OPTION_INPUT_PATH = "i";

  /**
   * Option to declare path to output graph
   */
  private static final String OPTION_OUTPUT_PATH = "o";

  /**
   * Option to declare output path to statistics csv file
   */
  private static final String OPTION_CSV_PATH = "c";

  /**
   * Option to declare grouping configuration
   */
  private static final String OPTION_GROUPING_CONFIG = "cf";

  /**
   * Option to declare table layout of output
   */
  private static final String OPTION_TABLE_LAYOUT = "l";

  /**
   * Used input path
   */
  private static String INPUT_PATH;

  /**
   * Used output path
   */
  private static String OUTPUT_PATH;

  /**
   * Used csv path
   */
  private static String CSV_PATH;

  /**
   * Used grouping configuration
   */
  private static int GROUPING_CONFIG;

  /**
   * Used table layout
   */
  private static String TABLE_LAYOUT;

  /**
   * True if table layout should be used
   */
  private static boolean USE_TABLE_LAYOUT;

  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true,
      "Path to indexed source files.");
    OPTIONS.addRequiredOption(OPTION_OUTPUT_PATH, "output", true,
      "Path to output file");
    OPTIONS.addRequiredOption(OPTION_CSV_PATH, "csv", true,
      "Path to csv statistics");
    OPTIONS.addRequiredOption(OPTION_GROUPING_CONFIG, "config", true, "Grouping configuration " +
      "number (1, 4 or 13)");
    OPTIONS.addOption(OPTION_TABLE_LAYOUT, "layout", true,
      "Table layout");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception in case of Error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TableGroupingBenchmark.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    // read cmd arguments
    readCMDArguments(cmd);

    // create gradoop config
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    List<String> vertexGroupingKeys = new ArrayList<>();
    List<AggregateFunction> vertexAggregationFunctions = new ArrayList<>();
    List<String> edgeGroupingKeys = new ArrayList<>();
    List<AggregateFunction> edgeAggregationFunctions = new ArrayList<>();

    switch (GROUPING_CONFIG) {
    case 1:
      vertexGroupingKeys.add(":label");
      break;
    case 4:
      vertexGroupingKeys.add(":label");
      vertexAggregationFunctions.add(new Count());
      edgeGroupingKeys.add(":label");
      edgeAggregationFunctions.add(new Count());
      break;
    case 13:
      vertexGroupingKeys.add("c");
      vertexAggregationFunctions.add(new Count());
      edgeAggregationFunctions.add(new Count());
      edgeAggregationFunctions.add(new MinProperty("since", "min_since"));
      edgeAggregationFunctions.add(new MaxProperty("since", "max_since"));
      break;
    default:
      throw new RuntimeException("Unsupported grouping configuration!");
    }

    if (USE_TABLE_LAYOUT) {
      setTableFactories(TABLE_LAYOUT, conf);
      // read graph
      TableCSVDataSource source = new TableCSVDataSource(INPUT_PATH, conf);
      TableLogicalGraph graph = source.getLogicalGraph();

      graph.groupBy(vertexGroupingKeys, vertexAggregationFunctions, edgeGroupingKeys,
        edgeAggregationFunctions)
        .writeTo(new TableCSVDataSink(OUTPUT_PATH, conf));
    } else {
      // read graph
      DataSource source = new CSVDataSource(INPUT_PATH, conf);
      LogicalGraph graph = source.getLogicalGraph();

      graph.groupBy(vertexGroupingKeys, vertexAggregationFunctions, edgeGroupingKeys,
        edgeAggregationFunctions, GroupingStrategy.GROUP_COMBINE)
        .writeTo(new CSVDataSink(OUTPUT_PATH, conf));
    }

    // execute and write job statistics
    env.execute();
    writeCSV(env);
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH        = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH       = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    CSV_PATH          = cmd.getOptionValue(OPTION_CSV_PATH);
    GROUPING_CONFIG   = Integer.parseInt(cmd.getOptionValue(OPTION_GROUPING_CONFIG));
    USE_TABLE_LAYOUT  = cmd.hasOption(OPTION_TABLE_LAYOUT);
    TABLE_LAYOUT      = cmd.getOptionValue(OPTION_TABLE_LAYOUT);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exeption during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {

    String head = String
      .format("%s|%s|%s|%s|%s%n", "grouping-config", "dataset", "parallelism", "layout",
        "Runtime(s)");

    String tail = String
      .format("%s|%s|%s|%s|%s%n", GROUPING_CONFIG, INPUT_PATH, env.getParallelism(),
        TABLE_LAYOUT, env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    File f = new File(CSV_PATH);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(CSV_PATH, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }
}
