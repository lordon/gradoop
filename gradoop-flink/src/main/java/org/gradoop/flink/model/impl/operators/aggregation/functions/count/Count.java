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
package org.gradoop.flink.model.impl.operators.aggregation.functions.count;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.Sum;

/**
 * Superclass of counting aggregate functions.
 */
public class Count extends BaseAggregateFunction implements Sum, AggregateDefaultValue {

  /**
   * Creates a new instance of a Count aggregate function.
   */
  public Count() {
    super("count");
  }

  /**
   * Creates a new instance of a Count aggregate function.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public Count(String aggregatePropertyKey) {
    super(aggregatePropertyKey);
  }

  @Override
  public PropertyValue getIncrement(Element element) {
    return PropertyValue.create(1L);
  }

  @Override
  public PropertyValue getDefaultValue() {
    return PropertyValue.create(0L);
  }
}
