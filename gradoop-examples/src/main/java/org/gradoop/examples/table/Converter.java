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
package org.gradoop.examples.table;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphCSVDataSource;
import org.gradoop.flink.io.impl.table.csv.TableCSVDataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Example program to convert graph collections / logical graphs by reading from a CSV Data Source
 * and writing into a Table CSV Data Sink
 */
public class Converter extends AbstractRunner implements ProgramDescription {

  /**
   * Option to declare path to indexed input graph
   */
  private static final String OPTION_INPUT_PATH = "i";

  /**
   * Option to declare path to output graph
   */
  private static final String OPTION_OUTPUT_PATH = "o";

  /**
   * Option to declare whether to convert a logical graph or a graph collection
   */
  private static final String OPTION_TYPE = "t";

  /**
   * Option to declare legacy CSV type
   */
  private static final String OPTION_LEGACY_INPUT_FORMAT = "li";

  /**
   * Option to declare table layout of output
   */
  private static final String OPTION_OUTPUT_TABLE_LAYOUT = "l";

  /**
   * Used input path
   */
  private static String INPUT_PATH;

  /**
   * Used output path
   */
  private static String OUTPUT_PATH;

  /**
   * Used table layout of output
   */
  private static String OUTPUT_TABLE_LAYOUT;

  /**
   * Type to convert (graph or collection)
   */
  private static String TYPE;

  /**
   * True iff legacy input format shall be used
   */
  private static boolean USE_LEGACY_INPUT_FORMAT;

  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true,
      "Path to indexed source files.");
    OPTIONS.addRequiredOption(OPTION_OUTPUT_PATH, "output", true,
      "Path to output file");
    OPTIONS.addRequiredOption(OPTION_OUTPUT_TABLE_LAYOUT, "layout", true,
      "Layout of output");
    OPTIONS.addRequiredOption(OPTION_TYPE, "type", true, "Type to convert");
    OPTIONS.addOption(OPTION_LEGACY_INPUT_FORMAT, "legacy-input-format", false,
      "Use legacy CSV input format");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception in case of Error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, Converter.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    // read cmd arguments
    readCMDArguments(cmd);

    // create gradoop config
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    setTableFactories(OUTPUT_TABLE_LAYOUT, conf);

    switch (TYPE) {
    case "logicalgraph":
      LogicalGraph logicalGraph;

      if (USE_LEGACY_INPUT_FORMAT) {
        logicalGraph = new LogicalGraphCSVDataSource(INPUT_PATH, conf).getLogicalGraph();
      } else {
        logicalGraph = new CSVDataSource(INPUT_PATH, conf).getLogicalGraph();
      }

      TableLogicalGraph tableLogicalGraph = conf.getTableLogicalGraphFactory()
        .fromDataSets(logicalGraph.getGraphHead(), logicalGraph.getVertices(),
          logicalGraph.getEdges());

      tableLogicalGraph.writeTo(new TableCSVDataSink(OUTPUT_PATH, conf));
      break;
    case "graphcollection":
      GraphCollection graphCollection = new CSVDataSource(INPUT_PATH, conf).getGraphCollection();

      TableGraphCollection tableGraphCollection = conf.getTableGraphCollectionFactory()
        .fromDataSets(graphCollection.getGraphHeads(), graphCollection.getVertices(),
          graphCollection.getEdges());

      tableGraphCollection.writeTo(new TableCSVDataSink(OUTPUT_PATH, conf));
      break;
    default:
      throw new RuntimeException("Unknown type: " + TYPE);
    }

    // execute
    env.execute();
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH   = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH  = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    OUTPUT_TABLE_LAYOUT = cmd.getOptionValue(OPTION_OUTPUT_TABLE_LAYOUT);
    USE_LEGACY_INPUT_FORMAT = cmd.hasOption(OPTION_LEGACY_INPUT_FORMAT);
    TYPE = cmd.getOptionValue(OPTION_TYPE);
  }

  @Override
  public String getDescription() {
    return Converter.class.getName();
  }
}
