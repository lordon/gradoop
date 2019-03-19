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
package org.gradoop.flink.model.api.layouts.table;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.TableSet;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base interface for all layouts using Flink's Table-API
 */
public interface TableLayout {

  /**
   * Returns table set of this layout
   * @return table set
   */
  TableSet getTableSet();

  /**
   * Returns a table set factory for this layout
   * @return table set factory
   */
  BaseTableSetFactory getTableSetFactory();

  /**
   * Returns current gradoop config
   * @return gradoop flink config
   */
  GradoopFlinkConfig getConfig();

  /**
   * Transforms data from tables into dataset of {@link Vertex}
   * @return dataset of {@link Vertex}
   */
  DataSet<Vertex> toVertexDataSet();

  /**
   * Transforms data from tables into dataset of {@link Edge}
   * @return dataset of {@link Edge}
   */
  DataSet<Edge> toEdgeDataSet();

  /**
   * Transforms data from tables into dataset of {@link GraphHead}
   * @return dataset of {@link GraphHead}
   */
  DataSet<GraphHead> toGraphHeadDataSet();

}
