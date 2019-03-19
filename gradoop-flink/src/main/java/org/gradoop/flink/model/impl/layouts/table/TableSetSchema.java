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
package org.gradoop.flink.model.impl.layouts.table;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.table.expressions.Expression;
import org.gradoop.flink.model.impl.layouts.table.util.ExpressionSeqBuilder;
import scala.collection.Seq;

import java.util.Map;
import java.util.Set;

/**
 * Wrapper for a table based EPGM schema, which is basically a map: tableName->{@link TableSchema}
 * The schema is meant to be constant and therefore realized with an {@link ImmutableMap}.
 */
public class TableSetSchema {

  /**
   * EPGM schema map
   */
  private ImmutableMap<String, TableSchema> schema;

  /**
   * Constructor
   * @param schema immutable schema map
   */
  public TableSetSchema(ImmutableMap<String, TableSchema> schema) {
    this.schema = schema;
  }

  /**
   * Returns true, iff the EPGM schema contains a table with given table name
   *
   * @param tableName table name to check
   * @return true, iff the EPGM schema contains a table with given table name
   */
  public boolean containsTable(String tableName) {
    return schema.containsKey(tableName);
  }

  /**
   * Returns the {@link TableSchema} for table with given table name
   *
   * @param tableName name of table to get schema for
   * @return table schema for table with given table name
   */
  public TableSchema getTable(String tableName) {
    if (!containsTable(tableName)) {
      throw new RuntimeException("Invalid tableName " + tableName);
    }
    return schema.get(tableName);
  }

  /**
   * Returns a set of tableName,TableSchema pairs
   *
   * @return set of pairs as {@link Map.Entry<String, TableSchema>}
   */
  public Set<Map.Entry<String, TableSchema>> getTables() {
    return schema.entrySet();
  }

  /**
   * Returns a string array of field names of table with given table name
   *
   * @param tableName name of table to get field names for
   * @return string array of field names
   */
  public String[] getFieldNamesForTable(String tableName) {
    return getTable(tableName).getFieldNames();
  }

  /**
   * Returns a string of comma separated fields names of table with given table name
   *
   * @param tableName name of table to get comma separated field names for
   * @return comma separated fields names
   */
  public String commaSeparatedFieldNamesForTable(String tableName) {
    return String.join(",", getFieldNamesForTable(tableName));
  }

  /**
   * Builds a scala sequence of expressions which can be used to project a table with a super set
   * of the fields (of the table for the given table name) to those fields only
   *
   * @param tableName name of table to get project expressions for
   * @return scala sequence of expressions
   */
  public Seq<Expression> buildProjectExpressions(String tableName) {
    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();
    for (String fieldName : getFieldNamesForTable(tableName)) {
      builder.field(fieldName);
    }
    return builder.buildSeq();
  }
}