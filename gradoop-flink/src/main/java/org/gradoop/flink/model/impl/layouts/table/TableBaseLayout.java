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

import org.gradoop.flink.model.api.layouts.table.TableLayout;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for all logical graph and graph collection layouts based on Flink's Table-API
 *
 * @param <TS> table set type
 * @param <TSF> table set factory type
 */
public abstract class TableBaseLayout<TS extends TableSet, TSF extends BaseTableSetFactory>
  implements TableLayout {

  /**
   * Java field name of EPGM element to access element's id
   */
  public static final String DATASET_EPGM_ELEMENT_ID_FIELD = "id";
  /**
   * Java field name of EPGM element to access element's label
   */
  public static final String DATASET_EPGM_ELEMENT_LABEL_FIELD = "label";
  /**
   * Java field name of EPGM element to access element's properties
   */
  public static final String DATASET_EPGM_ELEMENT_PROPERTIES_FIELD = "properties";
  /**
   * Java field name of EPGM graph element to access element's graph id set
   */
  public static final String DATASET_EPGM_GRAPH_ELEMENT_GRAPHIDS_FIELD = "graphIds";
  /**
   * Java field name of EPGM edge to access source id of an edge
   */
  public static final String DATASET_EPGM_EDGE_SOURCE_ID_FIELD = "sourceId";
  /**
   * Java field name of EPGM edge to access target id of an edge
   */
  public static final String DATASET_EPGM_EDGE_TARGET_ID_FIELD = "targetId";

  /**
   * Configuration
   */
  protected GradoopFlinkConfig config;

  /**
   * Table set the layout is based on
   */
  protected TS tableSet;

  /**
   * Factory used to product instances of table set the layout is based on
   */
  protected TSF tableSetFactory;

  /**
   * Constructor
   * @param tableSet table set the layout is based on
   * @param config gradoop config
   */
  public TableBaseLayout(TS tableSet, GradoopFlinkConfig config) {
    this.tableSet = tableSet;
    this.config = config;
  }

  @Override
  public GradoopFlinkConfig getConfig() {
    return config;
  }

  @Override
  public TS getTableSet() {
    return this.tableSet;
  }

  @Override
  public TSF getTableSetFactory() {
    return this.tableSetFactory;
  }

}
