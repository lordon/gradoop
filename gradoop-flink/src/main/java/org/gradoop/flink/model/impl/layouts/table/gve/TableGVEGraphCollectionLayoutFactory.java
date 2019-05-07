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
package org.gradoop.flink.model.impl.layouts.table.gve;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.table.TableGraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.table.TableGraphCollectionLayoutFactory;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.layouts.table.TableSet;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;

/**
 * Responsible for creating instances of {@link TableGVEGraphCollectionLayout}
 */
public class TableGVEGraphCollectionLayoutFactory extends TableGVEBaseLayoutFactory
  implements TableGraphCollectionLayoutFactory<GVETableSet> {

  @Override
  public TableGraphCollectionLayout fromTableSet(TableSet tableSet) {
    return new TableGVEGraphCollectionLayout(new GVETableSet(tableSet), getConfig());
  }

  @Override
  public TableGraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices, DataSet<Edge> edges) {
    GVETableSet tableSetFromDataSets = tableSetfromDataSets(graphHeads, vertices, edges);
    return new TableGVEGraphCollectionLayout(tableSetFromDataSets, getConfig());
  }

  @Override
  public TableGraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices) {
    return fromDataSets(graphHeads, vertices, createEdgeDataSet(new ArrayList<>(0)));
  }

  @Override
  public TableGraphCollectionLayout fromGraph(TableLogicalGraph logicalGraphLayout) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public TableGraphCollectionLayout fromGraphs(TableLogicalGraph... logicalGraphLayout) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public TableGraphCollectionLayout createEmptyCollection() {
    throw new NotImplementedException("Not implemented");
  }


}
