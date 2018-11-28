package org.gradoop.flink.model.api.epgm;

/**
 * Defines the base operators that are available on a {@link BaseGraph}.
 *
 * @param <LG> Logical graph type
 * @param <GC> Graph collection type
 * @param <DS> Data sink type
 */
public interface LogicalGraphBaseOperators<LG extends BaseGraph,
                                           GC extends BaseGraphCollection,
                                           DS>
  extends GraphBaseOperators<DS> {

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a new logical graph by combining the vertex and edge sets of
   * this graph and the given graph. Vertex and edge equality is based on their
   * identifiers.
   *
   * @param otherGraph logical graph to combine this graph with
   * @return logical graph containing all vertices and edges of the
   * input graphs
   */
  LG combine(LG otherGraph);

  /**
   * Creates a new logical graph containing the overlapping vertex and edge
   * sets of this graph and the given graph. Vertex and edge equality is
   * based on their identifiers.
   *
   * @param otherGraph logical graph to compute overlap with
   * @return logical graph that contains all vertices and edges that exist in
   * both input graphs
   */
  LG overlap(LG otherGraph);

  /**
   * Creates a new logical graph containing only vertices and edges that
   * exist in that graph but not in the other graph. Vertex and edge equality
   * is based on their identifiers.
   *
   * @param otherGraph logical graph to exclude from that graph
   * @return logical that contains only vertices and edges that are not in
   * the other graph
   */
  LG exclude(LG otherGraph);
}
