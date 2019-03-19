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
package org.gradoop.flink.model.api.epgm.table;

import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraphBaseOperators;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;

import java.util.List;
import java.util.Objects;

/**
 * Defines the operators that are available on a {@link BaseGraph}.
 *
 * @param <LG> Logical graph type
 * @param <GC> Graph collection type
 * @param <DS> Data sink type
 */
public interface TableLogicalGraphOperators<LG extends BaseGraph,
                                            GC extends BaseGraphCollection,
                                            DS>
  extends LogicalGraphBaseOperators<LG, GC, DS> {

  /**
   * Returns the subgraph that is induced by the vertices which labels are in given label set
   *
   * @param vertexLabels set of vertex labels
   * @return vertex-induced subgraph as a new logical graph
   */
  LG vertexInducedSubgraph(String... vertexLabels);

  /**
   * Returns the subgraph that is induced by the edges which labels are in given label set
   *
   * @param edgeLabels set of edge labels
   * @return edge-induced subgraph as a new logical graph
   */
  LG edgeInducedSubgraph(String... edgeLabels);

  /**
   * Returns a subgraph of the logical graph which contains only those vertices
   * and edges that match given label sets respectively.
   *
   * Note, that the operator does not verify the consistency of the resulting
   * graph. Use {#toGellyGraph().subgraph()} for that behaviour.
   *
   * @param vertexLabels  array of vertex labels
   * @param edgeLabels    array of edge labels
   * @return  logical graph which fulfils the given label predicates and is a subgraph
   *          of that graph
   */
  default LG subgraph(String[] vertexLabels, String[] edgeLabels) {
    Objects.requireNonNull(vertexLabels);
    Objects.requireNonNull(edgeLabels);
    return subgraph(vertexLabels, edgeLabels, Subgraph.Strategy.BOTH);
  }

  /**
   * Returns a subgraph of the logical graph which contains only those vertices
   * and edges that match given label sets respectively.
   *
   * Note, that the operator does not verify the consistency of the resulting
   * graph. Use {#toGellyGraph().subgraph()} for that behaviour.
   *
   * @param vertexLabels  array of vertex labels
   * @param edgeLabels    array of edge labels
   * @param strategy              execution strategy for the operator
   * @return  logical graph which fulfils the given label predicates and is a subgraph
   *          of that graph
   */
  LG subgraph(String[] vertexLabels, String[] edgeLabels, Subgraph.Strategy strategy);

  /**
   * Creates a condensed version of the logical graph by grouping vertices based on the specified
   * property keys.
   *
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices.
   *
   * Note: To group vertices by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   *
   * @return summary graph
   * @see Grouping
   */
  LG groupBy(List<String> vertexGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices and edges based on given
   * property keys.
   *
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys.
   *
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   *
   * Note: To group vertices/edges by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param edgeGroupingKeys property keys to group edges
   *
   * @return summary graph
   * @see Grouping
   */
  LG groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys);

  /**
   * Creates a condensed version of the logical graph by grouping vertices and edges based on given
   * property keys.
   *
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys. Furthermore, one can
   * specify sets of vertex and edge aggregate functions which are applied on vertices/edges
   * represented by the same super vertex/edge.
   *
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   *
   * Note: To group vertices/edges by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys property keys to group edges
   * @param edgeAggregateFunctions aggregate functions to apply on super edges
   *
   * @return summary graph
   * @see Grouping
   */
  LG groupBy(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions);

}
