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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import scala.collection.Seq;

/**
 * Utils methods for general usage of Flinks's Table API
 */
public class TableUtils {

  /**
   * Transform given table to a queryable result which is not recalculated on each reference.
   * This is done by switching to dataset api and back again.
   *
   * @param tableEnv current flink table execution environment
   * @param table table to make queryable
   * @return queryable table (result)
   */
  public static Table transformToQueryableResultTable(BatchTableEnvironment tableEnv, Table table) {
    String[] fieldNames = table.getSchema().getFieldNames();
    String fields = String.join(",", fieldNames);
    return tableEnv.fromDataSet(tableEnv.toDataSet(table, Row.class), fields);
  }

  /**
   * Register a dataset in table environment. Since Flink does not provide a method accepting a
   * sequence of expression for fields, this method is implemented using Flinks
   * {@link BatchTableEnvironment#registerDataSetInternal(String, DataSet, Expression[])}
   *
   * @param tableEnv current flink table execution environment
   * @param dataSet data set to register
   * @param fields scala sequence of field references / names
   * @return table representing the dataset
   */
  public static Table registerDataSet(BatchTableEnvironment tableEnv, DataSet dataSet,
    Seq<Expression> fields) {
    Expression[] exprs =
      (Expression[]) fields.toArray(scala.reflect.ClassTag$.MODULE$.apply(Expression.class));
    String name = tableEnv.createUniqueTableName();
    tableEnv.registerDataSetInternal(name, dataSet, exprs);
    return tableEnv.scan(name);
  }
}
