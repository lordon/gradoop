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
import org.gradoop.flink.model.api.layouts.BaseLayoutFactory;
import org.gradoop.flink.model.impl.layouts.table.TableSet;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;

/**
 * Base interface for all layout factories producing layouts using Flink's Table-API
 * @param <T> table set type
 */
public interface TableBaseLayoutFactory<T extends TableSet> extends BaseLayoutFactory {

  /**
   * Returns schema of this table layout implementation
   * @return schema of this table layout
   */
  TableSetSchema getSchema();

  /**
   * Transforms given GVE dataset's into a table set as defined in this layout
   *
   * @param graphHeads dataset of {@link GraphHead}
   * @param vertices dataset of {@link Vertex}
   * @param edges dataset of {@link Edge}
   * @return table set
   */
  T tableSetfromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges);

}
