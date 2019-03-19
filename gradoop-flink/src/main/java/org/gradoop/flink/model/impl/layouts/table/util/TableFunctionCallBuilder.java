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
package org.gradoop.flink.model.impl.layouts.table.util;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.plan.logical.LogicalTableFunctionCall;

import static org.gradoop.flink.model.impl.layouts.table.util.ExpressionUtils.convertStringArrayToFieldReferenceArray;

/**
 * Builder for table function calls in Flink's Table-API.
 *
 * Example usage:
 * <pre>
 *   {@code
 *      myTable.join(new Table(tableEnv, TableFunctionCallBuilder
 *           .build(new myTableFunction(), tableAlias, parameters...)
 *      ))
 *   }
 * </pre>
 */
public class TableFunctionCallBuilder {

  /**
   * Returns a {@link LogicalTableFunctionCall} instance to to pass to {@link Table} constructor
   * in order to perform a table function call. Given expression parameters are passed to table
   * function and result gets given alias name.
   *
   * @param function flink table function
   * @param alias alias name for table
   * @param parameters parameters as expressions
   * @return logical table function call to pass to {@link Table} constructor
   */
  public static LogicalTableFunctionCall build(TableFunction function, String alias,
    Expression... parameters) {
    return new LogicalTableFunctionCall(
      function.functionIdentifier(),
      function,
      ExpressionUtils.convertToSeq(parameters),
      function.getResultType(),
      new String[]{ alias },
      null);
  }

  /**
   * Returns a {@link LogicalTableFunctionCall} instance to to pass to {@link Table} constructor
   * in order to perform a table function call. Given field name parameters are passed to table
   * function and result gets given alias name.
   *
   * @param function flink table function
   * @param alias alias name for table
   * @param parameters parameters as field names
   * @return logical table function call to pass to {@link Table} constructor
   */
  public static LogicalTableFunctionCall build(TableFunction function, String alias,
    String... parameters) {
    return build(function, alias,
      convertStringArrayToFieldReferenceArray(parameters));
  }
}
