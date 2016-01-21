/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.UnaryFunction;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

import java.util.List;

/**
 * Maps the vertices to Tuple2's, where each tuple contains the vertex
 * and one split values the split values are determined using a user defined
 * function
 *
 * @param <V> EPGM vertex type
 */
public class SplitValues<V extends EPGMVertex>
  implements FlatMapFunction<V, Tuple2<GradoopId, PropertyValue>> {
  /**
   * Self defined Function
   */
  private UnaryFunction<V, List<PropertyValue>> function;

  /**
   * Constructor
   *
   * @param function actual defined Function
   */
  public SplitValues(
    UnaryFunction<V, List<PropertyValue>> function) {
    this.function = function;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(V vertex,
    Collector<Tuple2<GradoopId, PropertyValue>> collector) throws Exception {
    List<PropertyValue> splitValues = function.execute(vertex);
    for (PropertyValue value : splitValues) {
      collector.collect(new Tuple2<>(vertex.getId(), value));
    }
  }
}
