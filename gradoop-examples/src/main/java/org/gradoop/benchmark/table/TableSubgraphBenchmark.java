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
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.table.csv.TableCSVDataSink;
import org.gradoop.flink.io.impl.table.csv.TableCSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized subgraph benchmark. Both, table based and dataset based
 * execution is supported.
 */
public class TableSubgraphBenchmark extends AbstractRunner {

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
   * Option to declare verification
   */
  private static final String OPTION_VERIFICATION = "v";

  /**
   * Option to declare table layout of output
   */
  private static final String OPTION_TABLE_LAYOUT = "l";

  /**
   * Option to declare used vertex label
   */
  private static final String OPTION_VERTEX_LABELS = "vl";

  /**
   * Option to declare used edge label
   */
  private static final String OPTION_EDGE_LABELS = "el";

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
   * Used vertex label
   */
  private static String[] VERTEX_LABELS;

  /**
   * Used vertex label
   */
  private static String[] EDGE_LABELS;

  /**
   * Used table layout
   */
  private static String TABLE_LAYOUT;

  /**
   * True if table layout should be used
   */
  private static boolean USE_TABLE_LAYOUT;

  /**
   * Used verification flag
   */
  private static boolean VERIFICATION;

  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true,
      "Path to indexed source files.");
    OPTIONS.addRequiredOption(OPTION_OUTPUT_PATH, "output", true,
      "Path to output file");
    OPTIONS.addRequiredOption(OPTION_CSV_PATH, "csv", true,
      "Path to csv statistics");
    OPTIONS.addOption(Option.builder(OPTION_VERTEX_LABELS)
      .longOpt("vertex-labels")
      .hasArgs()
      .desc("Used vertex labels")
      .build());
    OPTIONS.addOption(Option.builder(OPTION_EDGE_LABELS)
      .longOpt("edge-labels")
      .hasArgs()
      .desc("Used edge labels")
      .build());
    OPTIONS.addOption(OPTION_VERIFICATION, "verification", false,
      "Verify Subgraph with join.");
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
    CommandLine cmd = parseArguments(args, TableSubgraphBenchmark.class.getName());

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
      // read graph
      TableCSVDataSource source = new TableCSVDataSource(INPUT_PATH, conf);
      TableLogicalGraph graph = source.getLogicalGraph();

      if (EDGE_LABELS == null) {
        graph = graph.vertexInducedSubgraph(VERTEX_LABELS);
      } else if (VERTEX_LABELS == null) {
        graph = graph.edgeInducedSubgraph(EDGE_LABELS);
      } else if (VERIFICATION) {
        graph = graph.subgraph(VERTEX_LABELS, EDGE_LABELS, Subgraph.Strategy.BOTH_VERIFIED);
      } else {
        graph = graph.subgraph(VERTEX_LABELS, EDGE_LABELS);
      }

      TableCSVDataSink sink = new TableCSVDataSink(OUTPUT_PATH, conf);
      sink.write(graph);

    } else {
      // read graph
      DataSource source = new CSVDataSource(INPUT_PATH, conf);
      LogicalGraph graph = source.getLogicalGraph();

      if (EDGE_LABELS == null) {
        graph = graph.vertexInducedSubgraph(new LabelIsIn<>(VERTEX_LABELS));
      } else if (VERTEX_LABELS == null) {
        graph = graph.edgeInducedSubgraph(new LabelIsIn<>(EDGE_LABELS));
      } else if (VERIFICATION) {
        graph = graph.subgraph(new LabelIsIn<>(VERTEX_LABELS), new LabelIsIn<>(EDGE_LABELS),
          Subgraph.Strategy.BOTH_VERIFIED);
      } else {
        graph = graph.subgraph(new LabelIsIn<>(VERTEX_LABELS), new LabelIsIn<>(EDGE_LABELS));
      }

      // write graph
      DataSink sink = new CSVDataSink(OUTPUT_PATH, conf);
      sink.write(graph);
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
    VERTEX_LABELS     = cmd.getOptionValues(OPTION_VERTEX_LABELS);
    EDGE_LABELS       = cmd.getOptionValues(OPTION_EDGE_LABELS);
    USE_TABLE_LAYOUT  = cmd.hasOption(OPTION_TABLE_LAYOUT);
    TABLE_LAYOUT      = cmd.getOptionValue(OPTION_TABLE_LAYOUT);
    VERIFICATION      = cmd.hasOption(OPTION_VERIFICATION);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exeption during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {

    String head = String
      .format("%s|%s|%s|%s|%s|%s|%s%n", "vertex-labels", "edge-labels", "verification", "dataset",
        "parallelism", "layout", "Runtime(s)");

    String tail = String
      .format("%s|%s|%s|%s|%s|%s|%s%n", Arrays.toString(VERTEX_LABELS),
        Arrays.toString(EDGE_LABELS), VERIFICATION, INPUT_PATH,
        env.getParallelism(),
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
