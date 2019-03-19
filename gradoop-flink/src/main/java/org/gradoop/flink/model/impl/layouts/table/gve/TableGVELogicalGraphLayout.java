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

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.api.table.TableDataSink;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayout;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.layouts.table.gve.operators.Combination;
import org.gradoop.flink.model.impl.layouts.table.gve.operators.Exclusion;
import org.gradoop.flink.model.impl.layouts.table.gve.operators.Grouping;
import org.gradoop.flink.model.impl.layouts.table.gve.operators.Overlap;
import org.gradoop.flink.model.impl.layouts.table.gve.operators.Subgraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * GVE layout of {@link TableLogicalGraph}
 */
public class TableGVELogicalGraphLayout extends TableGVELayout implements
  TableLogicalGraphLayout {

  /**
   * Constructor
   *
   * @param tableSet GVE table set
   * @param config gradoop configuration
   */
  public TableGVELogicalGraphLayout(GVETableSet tableSet, GradoopFlinkConfig config) {
    super(tableSet, config);
  }

  @Override
  public TableLogicalGraph vertexInducedSubgraph(String... vertexLabels) {
    return new Subgraph(vertexLabels, ArrayUtils.EMPTY_STRING_ARRAY,
      org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.VERTEX_INDUCED)
      .execute(this);
  }

  @Override
  public TableLogicalGraph edgeInducedSubgraph(String... edgeLabels) {
    return new Subgraph(ArrayUtils.EMPTY_STRING_ARRAY, edgeLabels,
      org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.EDGE_INDUCED)
      .execute(this);
  }

  @Override
  public TableLogicalGraph subgraph(String[] vertexLabels, String[] edgeLabels,
    org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy strategy) {
    return new Subgraph(vertexLabels, edgeLabels, strategy).execute(this);
  }


  @Override
  public TableLogicalGraph groupBy(List<String> vertexGroupingKeys) {
    return groupBy(vertexGroupingKeys, null);
  }

  @Override
  public TableLogicalGraph groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    return groupBy(vertexGroupingKeys, null, edgeGroupingKeys, null);
  }

  @Override
  public TableLogicalGraph groupBy(List<String> vertexGroupingKeys,
    List<AggregateFunction> vertexAggregateFunctions, List<String> edgeGroupingKeys,
    List<AggregateFunction> edgeAggregateFunctions) {
    Objects.requireNonNull(vertexGroupingKeys, "missing vertex grouping key(s)");

    Grouping.GroupingBuilder builder = new Grouping.GroupingBuilder();

    builder.addVertexGroupingKeys(vertexGroupingKeys);

    if (edgeGroupingKeys != null) {
      builder.addEdgeGroupingKeys(edgeGroupingKeys);
    }

    if (vertexAggregateFunctions != null) {
      for (AggregateFunction f : vertexAggregateFunctions) {
        builder.addVertexAggregateFunction(f);
      }
    }

    if (edgeAggregateFunctions != null) {
      for (AggregateFunction f : edgeAggregateFunctions) {
        builder.addEdgeAggregateFunction(f);
      }
    }

    return builder.build().execute(this);
  }

  @Override
  public TableLogicalGraph combine(TableLogicalGraph otherGraph) {
    return new Combination().execute(this, otherGraph.getLayout());
  }

  @Override
  public TableLogicalGraph overlap(TableLogicalGraph otherGraph) {
    return new Overlap().execute(this, otherGraph.getLayout());
  }

  @Override
  public TableLogicalGraph exclude(TableLogicalGraph otherGraph) {
    return new Exclusion().execute(this, otherGraph.getLayout());
  }

  @Override
  public void writeTo(TableDataSink dataSink) throws IOException {
    dataSink.write(this);
  }

  @Override
  public void writeTo(TableDataSink dataSink, boolean overWrite) throws IOException {
    dataSink.write(this, overWrite);
  }

  @Override
  public DataSet<Boolean> isEmpty() {
    throw new NotImplementedException();
  }
}
