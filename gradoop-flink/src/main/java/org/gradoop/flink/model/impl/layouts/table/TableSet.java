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

import org.apache.flink.table.api.Table;
import org.gradoop.flink.io.impl.table.csv.TableCSVDataSource;
import org.gradoop.flink.model.api.layouts.table.BaseTableSet;

import java.util.HashMap;

/**
 * Basic table set class which is just a wrapper for a map: tableName->{@link Table}
 *
 * The tableName is a name in Gradoop context, like "vertices". It must not be confused with the
 * internal table name in Flink's table environment!
 *
 * This basic class may be initialized in generic contexts, where the actual table set
 * implementation is to specific, like in the generic {@link TableCSVDataSource}
 */
public class TableSet extends HashMap<String, Table> implements BaseTableSet {

  /**
   * Empty constructor
   */
  public TableSet() { }

  /**
   * Constructor. Each entry of the given table set is added to the newly created one.
   *
   * @param tableSet table set to get new entries from
   */
  public TableSet(TableSet tableSet) {
    putAll(tableSet);
  }

}
