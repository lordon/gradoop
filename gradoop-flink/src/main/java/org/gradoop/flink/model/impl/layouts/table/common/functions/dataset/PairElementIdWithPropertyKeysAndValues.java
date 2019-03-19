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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Pairs given element with property name and property value for each property
 *
 * @param <T> epgm element type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class PairElementIdWithPropertyKeysAndValues<T extends EPGMElement> implements
  FlatMapFunction<T, Tuple3<GradoopId, String, PropertyValue>> {

  /**
   * Reduce instantiations.
   */
  private final Tuple3<GradoopId, String, PropertyValue> reuseTuple = new Tuple3<>();

  @Override
  public void flatMap(T t, Collector<Tuple3<GradoopId, String, PropertyValue>> collector) {
    if (null != t.getProperties()) {
      for (Property p : t.getProperties()) {
        reuseTuple.f0 = t.getId();
        reuseTuple.f1 = p.getKey();
        reuseTuple.f2 = p.getValue();
        collector.collect(reuseTuple);
      }
    }
  }
}