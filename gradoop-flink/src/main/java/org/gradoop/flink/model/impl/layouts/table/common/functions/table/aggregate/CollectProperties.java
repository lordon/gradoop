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
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar.ToProperties;

/**
 * Aggregation function to collect a set of {@link Row} in a certain format and accumulate them
 * in a {@link Properties} instance
 */
public class CollectProperties extends AggregateFunction<Properties, Properties> {

  @Override
  public Properties createAccumulator() {
    return Properties.create();
  }

  @Override
  public Properties getValue(Properties properties) {
    return properties;
  }

  /**
   * Processes the input row and update the provided properties instance.
   * Requires a 2n-ary Row
   * | key_1:String | value_1:PropertyValue | ... | key_n:String | value_n:PropertyValue|
   *
   * @param accumulator   the properties which contains the current aggregated results
   * @param value         the input row
   */
  public void accumulate(Properties accumulator, Row value) {
    ToProperties.processPropertiesRow(accumulator, value);
  }

  /**
   * Merges a group of properties instances into one properties instance.
   *
   * @param accumulator  the gradoop id set which will keep the merged aggregate results.
   * @param its          an [[java.lang.Iterable]] pointed to a group of properties instances
   */
  public void merge(Properties accumulator, Iterable<Properties> its) {
    for (Properties props : its) {
      for (Property prop : props) {
        accumulator.set(prop.getKey(), prop.getValue());
      }
    }
  }

  /**
   * Resets the properties instance.
   *
   * @param accumulator the properties instance which needs to be reset
   */
  public void resetAccumulator(Properties accumulator) {
    accumulator.clear();
  }

  @Override
  public TypeInformation getResultType() {
    return TypeInformation.of(Properties.class);
  }
}
