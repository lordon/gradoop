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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.flink.model.impl.layouts.table.TableSchema;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import org.gradoop.flink.util.GradoopFlinkConfig;
import scala.collection.Seq;

import java.io.File;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for table csv data sink and source
 */
public abstract class TableCSVBase {

  /**
   * File ending for CSV files.
   */
  protected static final String CSV_FILE_SUFFIX = ".csv";

  /**
   * Field delimiter of CSV files.
   */
  protected static final String CSV_FIELD_DELIMITER = ",";

  /**
   * Root directory containing the CSV and metadata files.
   */
  protected final String csvRoot;

  /**
   * Gradoop Flink configuration
   */
  protected final GradoopFlinkConfig config;

  /**
   * Constructor.
   *
   * @param csvPath directory to the CSV files
   * @param config Gradoop Flink configuration
   */
  protected TableCSVBase(String csvPath, GradoopFlinkConfig config) {
    Objects.requireNonNull(csvPath);
    Objects.requireNonNull(config);
    this.csvRoot = csvPath.endsWith(File.separator) ? csvPath : csvPath + File.separator;
    this.config = config;
  }

  /**
   * Builds a scala sequence of expressions to project a table to its fields and call scalar
   * functions on that fields if fields data type is mapped to certain scalar function in given
   * mapping
   *
   * @param tableSchema schema of table to build expressions sequence for
   * @param classFunctionMapping map containing scalar function for specific class
   * @return Scala sequence of expressions
   */
  protected Seq<Expression> buildProjectExpressions(TableSchema tableSchema,
    Map<Class, ScalarFunction> classFunctionMapping) {

    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();
    for (Map.Entry<String, TypeInformation> field : tableSchema.getFields()) {
      String fieldName = field.getKey();
      TypeInformation fieldType = field.getValue();

      /*
        Call scalar function on field if there is a mapping for corresponding fieldType class in
        classFunctionMapping
      */

      for (Map.Entry<Class, ScalarFunction> typeWithScalarFuction :
        classFunctionMapping.entrySet()) {
        TypeInformation paramTypeOfScalarFunction =
          TypeInformation.of(typeWithScalarFuction.getKey());
        ScalarFunction scalarFunction = typeWithScalarFuction.getValue();

        if (fieldType.equals(paramTypeOfScalarFunction)) {
          builder.scalarFunctionCall(scalarFunction, fieldName).as(fieldName);
          continue;
        }
      }
      builder.field(fieldName);
    }
    return builder.buildSeq();
  }
}
