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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Filters {@link Tuple3<GradoopId, String, PropertyValue>} by specified property key string.
 */
public class ByPropertyKey implements FilterFunction<Tuple3<GradoopId, String, PropertyValue>> {

  /**
   * Property key to filter on
   */
  private String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey property key to filter on
   */
  public ByPropertyKey(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public boolean filter(
    Tuple3<GradoopId, String, PropertyValue> gradoopIdStringPropertyValueTuple3) throws Exception {
    return gradoopIdStringPropertyValueTuple3.f1.equals(this.propertyKey);
  }
}
