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
package org.gradoop.flink.model.impl.layouts.table.common.functions.table.scalar;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Takes a row in special format and returns a properties instance
 */
public class ToProperties extends ScalarFunction {

  /**
   * Requires a 2n-ary Row
   * | key_1:String | value_1:PropertyValue | ... | key_n:String | value_n:PropertyValue|
   *
   * @param row row containing property key-value pairs
   * @return properties instance
   */
  public Properties eval(Row row) {
    Properties properties = Properties.create();
    properties = processPropertiesRow(properties, row);
    return properties;
  }

  /**
   * Receives a 2n-ary Row
   * | key_1:String | value_1:PropertyValue | ... | key_n:String | value_n:PropertyValue|
   * and adds each key-value pair to given Properties instance
   *
   * @param properties Properties instance to add new key-value pairs to
   * @param o 2n-ary Row containing keys and values
   * @return properties instance with newly added entries
   */
  public static Properties processPropertiesRow(Properties properties, Object o) {
    if (!(o instanceof Row)) {
      throw new RuntimeException("Passing a " + o.getClass().getName() + " is not allowed here");
    }

    Row row = (Row) o;

    if (row.getArity() % 2 != 0) {
      throw new RuntimeException("Arity of given row is odd and therefore can't get processed");
    }

    for (int i = 0; i < row.getArity(); i = i + 2) {
      Object f0 = row.getField(i);
      if (!(f0 instanceof String)) {
        throw new RuntimeException("Odd expression of property row must be a property key string");
      }
      Object f1 = row.getField(i + 1);
      if (!(f1 instanceof PropertyValue)) {
        throw new RuntimeException("Even expression of property row must be a property value");
      }

      String key = (String) f0;
      PropertyValue value = (PropertyValue) f1;

      properties.set(key, value);
    }
    return properties;
  }

}
