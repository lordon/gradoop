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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Base class for all property value based table aggregation functions
 */
public abstract class BaseTablePropertyValueAggregateFunction
  extends AggregateFunction<PropertyValue, PropertyValue> {

  @Override
  public abstract PropertyValue createAccumulator();

  @Override
  public abstract PropertyValue getValue(PropertyValue propertyValue);

  /**
   * Processes the input property values and update the provided property value instance.
   *
   * @param accumulator  the property value which contains the current aggregated results
   * @param value        the input property value
   */
  public abstract void accumulate(PropertyValue accumulator, PropertyValue value);

  /**
   * Merges a group of property value instances into one property value.
   *
   * @param accumulator  the property value which will keep the merged aggregate results.
   * @param its          an [[java.lang.Iterable]] pointed to a group of property value instances
   *                     that will be merged.
   */
  public abstract void merge(PropertyValue accumulator, Iterable<PropertyValue> its);

  /**
   * Resets the property value.
   *
   * @param accumulator  the property value which needs to be reset
   */
  public abstract void resetAccumulator(PropertyValue accumulator);

  @Override
  public TypeInformation getResultType() {
    return TypeInformation.of(PropertyValue.class);
  }

}
