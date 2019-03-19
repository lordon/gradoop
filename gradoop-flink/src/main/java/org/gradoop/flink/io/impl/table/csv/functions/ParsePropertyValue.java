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
package org.gradoop.flink.io.impl.table.csv.functions;

import org.apache.flink.table.functions.ScalarFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Base64;

/**
 * Takes Base64 encoded string and returns decoded property value
 */
public class ParsePropertyValue extends ScalarFunction {

  /**
   * Returns decoded property value from given Base64 string
   *
   * @param s Base64 encoded string
   * @return decoded property value
   */
  public PropertyValue eval(String s) {
    return PropertyValue.fromRawBytes(Base64.getDecoder().decode(s));
  }

}
