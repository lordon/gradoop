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
package org.gradoop.flink.model.impl.layouts.table.horizontal;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayout;
import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.layouts.table.TableSet;

import java.util.ArrayList;

/**
 * Responsible for creating instances of {@link TableHorizontalLogicalGraphLayout}
 */
public class TableHorizontalLogicalGraphLayoutFactory extends TableHorizontalBaseLayoutFactory
  implements TableLogicalGraphLayoutFactory<HorizontalTableSet> {

  @Override
  public TableLogicalGraphLayout fromTableSet(TableSet tableSet) {
    return new TableHorizontalLogicalGraphLayout(new HorizontalTableSet(tableSet), getConfig());
  }

  @Override
  public TableLogicalGraphLayout fromDataSets(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices, DataSet<Edge> edges) {
    HorizontalTableSet tableSetFromDataSets = tableSetfromDataSets(graphHeads, vertices, edges);
    return new TableHorizontalLogicalGraphLayout(tableSetFromDataSets, getConfig());
  }

  @Override
  public TableLogicalGraphLayout fromDataSets(DataSet<Vertex> vertices) {
    return fromDataSets(vertices, createEdgeDataSet(new ArrayList<>(0)));
  }

  @Override
  public TableLogicalGraphLayout fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges) {
    throw new NotImplementedException();
  }

  @Override
  public TableLogicalGraphLayout createEmptyGraph() {
    throw new NotImplementedException();
  }
}
