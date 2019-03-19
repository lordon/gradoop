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
package org.gradoop.flink.model.impl.layouts.table.common.operators.subgraph;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.table.api.Table;
import org.gradoop.flink.model.api.layouts.table.operators.TableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.common.operators.base.TableUnaryGraphToGraphOperatorBase;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;

import java.util.Objects;

import static org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet.FIELD_EDGE_LABEL;
import static org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet.FIELD_VERTEX_LABEL;
import static org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.EDGE_INDUCED_PROJECT_FIRST;

/**
 * Base class for table based subgraph implementation which are built upon a table set which
 * extends the GVE table set (there need to be at least three tables: vertices, edges, graphs)
 *
 * @param <TS> table set type
 * @param <TSF> table set factory type
 */
public abstract class TableSubgraphBase<TS extends GVETableSet, TSF extends BaseTableSetFactory>
  extends TableUnaryGraphToGraphOperatorBase<TS, TSF> implements TableUnaryGraphToGraphOperator {

  /**
   * Vertex labels
   */
  protected String[] vertexLabels;
  /**
   * Edge labels
   */
  protected String[] edgeLabels;
  /**
   * Strategy
   */
  protected org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy strategy;

  /**
   * Constructor
   *
   * @param vertexLabels vertex labels
   * @param edgeLabels edge labels
   * @param strategy strategy
   */
  public TableSubgraphBase(String[] vertexLabels, String[] edgeLabels,
    org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy strategy) {
    Objects.requireNonNull(vertexLabels);
    Objects.requireNonNull(edgeLabels);
    Objects.requireNonNull(strategy);

    this.vertexLabels = vertexLabels.clone();
    this.edgeLabels = edgeLabels.clone();
    this.strategy = strategy;
  }

  @Override
  protected TableLogicalGraph computeNewLogicalGraph() {
    Table newVertices;
    Table newEdges;

    switch (strategy) {
    case BOTH:
      newVertices = filterVertices();
      newEdges = filterEdges();
      break;
    case BOTH_VERIFIED:
      newVertices = filterVertices();
      newEdges = computeNewVertexInducedEdges(filterEdges(), newVertices);
      break;
    case VERTEX_INDUCED:
      newVertices = filterVertices();
      newEdges = computeNewVertexInducedEdges(tableSet.getEdges(), newVertices);
      break;
    case EDGE_INDUCED:
      newEdges = filterEdges();
      newVertices = computeNewEdgeInducedVertices(tableSet.getVertices(), newEdges);
      break;
    case EDGE_INDUCED_PROJECT_FIRST:
      throw new NotImplementedException("Strategy " + EDGE_INDUCED_PROJECT_FIRST + "not " +
        "implemented in table layout");
    default:
      throw new IllegalArgumentException("Strategy " + strategy + " is not supported");
    }

    TS tableSet = buildInducedTableSet(newVertices, newEdges);
    return config.getTableLogicalGraphFactory().fromTableSet(tableSet);
  }

  /**
   * Computes new  based on filtered vertices and edges
   *
   * @param vertices filtered vertices table
   * @param edges filtered edges table
   * @return table set of result logical graph
   */
  protected abstract TS buildInducedTableSet(Table vertices, Table edges);

  /**
   * Performs a selection by label on vertices table
   *
   * @return filtered vertices table
   */
  private Table filterVertices() {
    return tableSet.getVertices()
      .where(builder
        .field(FIELD_VERTEX_LABEL)
        .in(this.vertexLabels)
        .toExpression()
      );
  }

  /**
   * Performs a selection by label on edges table
   *
   * @return filtered edges table
   */
  private Table filterEdges() {
    return tableSet.getEdges()
      .where(builder
        .field(FIELD_EDGE_LABEL)
        .in(this.edgeLabels)
        .toExpression()
      );
  }

}
