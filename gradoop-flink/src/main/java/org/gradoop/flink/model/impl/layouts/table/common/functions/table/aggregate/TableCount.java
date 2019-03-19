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
package org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;

/**
 * Table Aggregate function to count elements
 */
public class TableCount extends BaseTablePropertyValueAggregateFunction {

  @Override
  public PropertyValue createAccumulator() {
    return PropertyValue.create(0L);
  }

  @Override
  public PropertyValue getValue(PropertyValue propertyValue) {
    return propertyValue;
  }

  @Override
  public void accumulate(PropertyValue acc, PropertyValue val) {
    acc.setObject(PropertyValueUtils.Numeric.add(acc, PropertyValue.create(1L)).getObject());
  }

  @Override
  public void merge(PropertyValue acc, Iterable<PropertyValue> it) {
    for (PropertyValue val : it) {
      acc.setObject(PropertyValueUtils.Numeric.add(acc, val).getObject());
    }
  }

  @Override
  public void resetAccumulator(PropertyValue acc) {
    acc.setLong(0L);
  }

}
