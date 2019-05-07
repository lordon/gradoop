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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * Base class for all EPGM set operator benchmarks.
 */
public abstract class TableSetOperatorBaseBenchmark extends AbstractRunner {

  /**
   * Used first input path
   */
  static String FIRST_INPUT_PATH;

  /**
   * Used second input path
   */
  static String SECOND_INPUT_PATH;

  /**
   * Used output path
   */
  static String OUTPUT_PATH;

  /**
   * Used table layout of output
   */
  static String TABLE_LAYOUT;

  /**
   * True if table layout should be used
   */
  static boolean USE_TABLE_LAYOUT;

  /**
   * Used csv path
   */
  private static String CSV_PATH;

  /**
   * Option to declare path to first input
   */
  private static final String OPTION_FIRST_INPUT_PATH = "i1";

  /**
   * Option to declare path to second input
   */
  private static final String OPTION_SECOND_INPUT_PATH = "i2";

  /**
   * Option to declare path to output
   */
  private static final String OPTION_OUTPUT_PATH = "o";

  /**
   * Option to declare output path to statistics csv file
   */
  private static final String OPTION_CSV_PATH = "c";

  /**
   * Option to declare table layout of output
   */
  private static final String OPTION_TABLE_LAYOUT = "l";

  static {
    OPTIONS.addOption(Option.builder(OPTION_FIRST_INPUT_PATH)
      .longOpt("input1")
      .hasArg()
      .desc("Path to first input graph/collection")
      .required()
      .build()
    );
    OPTIONS.addOption(Option.builder(OPTION_SECOND_INPUT_PATH)
      .longOpt("input2")
      .hasArg()
      .desc("Path to second input graph/collection")
      .required()
      .build()
    );
    OPTIONS.addOption(Option.builder(OPTION_OUTPUT_PATH)
      .longOpt("output")
      .hasArg()
      .desc("Path to output")
      .required()
      .build()
    );
    OPTIONS.addOption(Option.builder(OPTION_CSV_PATH)
      .longOpt("csv")
      .hasArg()
      .desc("Path to csv statistics")
      .required()
      .build()
    );
    OPTIONS.addOption(Option.builder(OPTION_TABLE_LAYOUT)
      .longOpt("layout")
      .hasArg()
      .desc("Table layout")
      .build()
    );
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  protected static void readCMDArguments(CommandLine cmd) {
    FIRST_INPUT_PATH        = cmd.getOptionValue(OPTION_FIRST_INPUT_PATH);
    SECOND_INPUT_PATH       = cmd.getOptionValue(OPTION_SECOND_INPUT_PATH);
    OUTPUT_PATH             = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    CSV_PATH                = cmd.getOptionValue(OPTION_CSV_PATH);
    USE_TABLE_LAYOUT        = cmd.hasOption(OPTION_TABLE_LAYOUT);
    TABLE_LAYOUT            = cmd.getOptionValue(OPTION_TABLE_LAYOUT);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exeption during file writing
   */
  protected static void writeCSV(ExecutionEnvironment env) throws IOException {
    String head = String
      .format("%s|%s|%s|%s%n", "dataset", "parallelism", "layout",
        "Runtime(s)");

    String tail = String
      .format("%s|%s|%s|%s%n", FIRST_INPUT_PATH, env.getParallelism(),
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
