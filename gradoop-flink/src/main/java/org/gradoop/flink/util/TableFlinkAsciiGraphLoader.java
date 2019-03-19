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
package org.gradoop.flink.util;

import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;

/**
 * Used to generate instances of {@link TableLogicalGraph} and {@link TableGraphCollection} from
 * GDL based on {@link FlinkAsciiGraphLoader}.
 */
public class TableFlinkAsciiGraphLoader extends FlinkAsciiGraphLoader {

  /**
   * Creates a new FlinkAsciiGraphLoader instance.
   *
   * @param config Gradoop Flink configuration
   */
  public TableFlinkAsciiGraphLoader(GradoopFlinkConfig config) {
    super(config);
  }

  /**
   * Builds a {@link TableGraphCollection} from the graph referenced by the given
   * graph variables.
   *
   * @param variables graph variables used in GDL script
   * @return TableGraphCollection
   */
  public TableGraphCollection getTableGraphCollectionByVariables(String... variables) {
    GraphCollection col = super.getGraphCollectionByVariables(variables);
    return config.getTableGraphCollectionFactory().fromDataSets(col.getGraphHeads(),
      col.getVertices(), col.getEdges());
  }

  /**
   * Builds a {@link TableLogicalGraph} from the graph referenced by the given
   * graph variable.
   *
   * @param variable graph variable used in GDL script
   * @return TableLogicalGraph
   */
  public TableLogicalGraph getTableLogicalGraphByVariable(String variable) {
    LogicalGraph lg = super.getLogicalGraphByVariable(variable);
    return config.getTableLogicalGraphFactory().fromDataSets(lg.getGraphHead(), lg.getVertices(),
      lg.getEdges());
  }

  /**
   * Returns a table logical graph containing the complete vertex and edge space of
   * the database.
   *
   * @return table logical graph of vertex and edge space
   */
  public TableLogicalGraph getTableLogicalGraph() {
    LogicalGraph lg = super.getLogicalGraph();
    return config.getTableLogicalGraphFactory().fromDataSets(lg.getGraphHead(), lg.getVertices(),
      lg.getEdges());
  }
}
