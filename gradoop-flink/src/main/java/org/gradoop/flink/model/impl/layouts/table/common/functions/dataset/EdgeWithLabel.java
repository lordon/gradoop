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
package org.gradoop.flink.model.impl.layouts.table.common.functions.dataset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Extracts id, sourceId, targetId and label from an {@link Edge}
 */
public class EdgeWithLabel implements MapFunction<Edge, Tuple4<GradoopId, GradoopId, GradoopId, String>> {

  /**
   * Reduce instantiations.
   */
  private final Tuple4<GradoopId, GradoopId, GradoopId, String> reuseTuple = new Tuple4<>();

  @Override
  public Tuple4<GradoopId, GradoopId, GradoopId, String> map(Edge edge) throws Exception {
    reuseTuple.f0 = edge.getId();
    reuseTuple.f1 = edge.getSourceId();
    reuseTuple.f2 = edge.getTargetId();
    reuseTuple.f3 =  edge.getLabel();
    return reuseTuple;
  }
}
