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
import org.gradoop.flink.model.impl.layouts.table.util.TableUtils;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for all factories of table sets
 */
public abstract class BaseTableSetFactory {

  /**
   * Configuration
   */
  protected GradoopFlinkConfig config;

  /**
   * Constructor
   *
   * @param config gradoop configuration
   */
  public BaseTableSetFactory(GradoopFlinkConfig config) {
    this.config = config;
  }

  /**
   * Transform given table to a queryable result which is not recalculated on each reference.
   * This is done by switching to dataset api and back again.
   *
   * @param table table to make queryable
   * @return queryable table (result)
   */
  protected Table transformToQueryableResultTable(Table table) {
    return TableUtils.transformToQueryableResultTable(config.getTableEnvironment(), table);
  }

}
