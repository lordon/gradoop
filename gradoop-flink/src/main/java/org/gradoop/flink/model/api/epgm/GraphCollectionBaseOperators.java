package org.gradoop.flink.model.api.epgm;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Defines the base operators that are available on a {@link BaseGraphCollection}.
 *
 * @param <LG> Logical graph type
 * @param <GC> Graph collection type
 * @param <DS> Data sink type
 */
public interface GraphCollectionBaseOperators<LG extends BaseGraph,
                                              GC extends BaseGraphCollection,
                                              DS> extends
  GraphBaseOperators<DS> {

  //----------------------------------------------------------------------------
  // Logical Graph / Graph Head Getters
  //----------------------------------------------------------------------------

  /**
   * Returns logical graph from collection using the given identifier. If the
   * graph does not exist, an empty logical graph is returned.
   *
   * @param graphID graph identifier
   * @return logical graph with given id or an empty logical graph
   */
  LG getGraph(final GradoopId graphID);
  /**
   * Extracts logical graphs from collection using their identifiers.
   *
   * @param identifiers graph identifiers
   * @return collection containing requested logical graphs
   */
  GC getGraphs(final GradoopId... identifiers);

  /**
   * Extracts logical graphs from collection using their identifiers.
   *
   * @param identifiers graph identifiers
   * @return collection containing requested logical graphs
   */
  GC getGraphs(GradoopIdSet identifiers);

  //----------------------------------------------------------------------------
  // Binary set operators
  //----------------------------------------------------------------------------

  /**
   * Returns a collection with all logical graphs from two input collections.
   * Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build union with
   * @return union of both collections
   */
  GC union(GC otherCollection);

  /**
   * Returns a collection with all logical graphs that exist in both input
   * collections. Graph equality is based on their identifiers.
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   */
  GC intersect(GC otherCollection);

  /**
   * Returns a collection with all logical graphs that exist in both input
   * collections. Graph equality is based on their identifiers.
   * <p>
   * Implementation that works faster if {@code otherCollection} is small
   * (e.g. fits in the workers main memory).
   *
   * @param otherCollection collection to build intersect with
   * @return intersection of both collections
   */
  GC intersectWithSmallResult(GC otherCollection);

  /**
   * Returns a collection with all logical graphs that are contained in that
   * collection but not in the other. Graph equality is based on their
   * identifiers.
   *
   * @param otherCollection collection to subtract from that collection
   * @return difference between that and the other collection
   */
  GC difference(GC otherCollection);

  /**
   * Returns a collection with all logical graphs that are contained in that
   * collection but not in the other. Graph equality is based on their
   * identifiers.
   * <p>
   * Alternate implementation that works faster if the intermediate result
   * (list of graph identifiers) fits into the workers memory.
   *
   * @param otherCollection collection to subtract from that collection
   * @return difference between that and the other collection
   */
  GC differenceWithSmallResult(GC otherCollection);
}
