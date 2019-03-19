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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Receives (id, element) and returns (element.id, id) where element is an {@link EPGMElement}
 * and id is a {@link GradoopId}
 *
 * @param <T> epgm element type
 */
@FunctionAnnotation.ForwardedFields("f0->f1")
public class SwitchPairAndExtractElementId<T extends EPGMElement> implements
  MapFunction<Tuple2<GradoopId, T>, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reduce instantiations.
   */
  private final Tuple2<GradoopId, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<GradoopId, GradoopId> map(Tuple2<GradoopId, T> graphIdElementPair) {
    reuseTuple.f0 = graphIdElementPair.f1.getId();
    reuseTuple.f1 = graphIdElementPair.f0;
    return reuseTuple;
  }
}
