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
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Map;
import java.util.Set;

/**
 * Wrapper for table schema which is based on a map: fieldName -> TypeInformation
 */
public class TableSchema {

  /**
   * Table schema map
   */
  private ImmutableMap<String, TypeInformation> tableSchema;

  /**
   * Constructor
   *
   * @param tableSchema table schema map
   */
  public TableSchema(ImmutableMap<String, TypeInformation> tableSchema) {
    this.tableSchema = tableSchema;
  }

  /**
   * Returns a string array of field names of this table
   *
   * @return string array of field names
   */
  public String[] getFieldNames() {
    return this.tableSchema.keySet().toArray(new String[0]);
  }

  /**
   * Returns a set of fieldName,TypeInformation pairs
   *
   * @return set of fields as {@link Map.Entry<String, TypeInformation>}
   */
  public Set<Map.Entry<String, TypeInformation>> getFields() {
    return this.tableSchema.entrySet();
  }
}
