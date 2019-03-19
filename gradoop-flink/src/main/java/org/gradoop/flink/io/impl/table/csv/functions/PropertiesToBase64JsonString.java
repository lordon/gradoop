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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.common.model.impl.properties.Properties;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Takes a {@link Properties} object and returns a Base64 encoded string representation of given
 * properties
 */
public class PropertiesToBase64JsonString extends ScalarFunction {

  /**
   * Returns a Base64 encoded string representation of given properties. The encoded string gets
   * build in the following way:
   *
   * - Property set is represented by a JSON object
   * - Property keys become JSON keys
   * - Property values get converted to Base64 strings and those strings become JSON values
   * - The whole JSON string gets Base64 encoded
   *
   * @param properties properties to encode
   * @return Base64 string
   * @throws JSONException
   */
  public String eval(Properties properties) throws JSONException {
    JSONObject data = new JSONObject();

    if (properties.size() > 0) {
      for (String propertyKey : properties.getKeys()) {
        byte[] encodedBytes =
          Base64.getEncoder().encode(properties.get(propertyKey).getRawBytes());
        data.put(propertyKey, new String(encodedBytes, StandardCharsets.ISO_8859_1));
      }
    }

    byte[] encodedBytes =
      Base64.getEncoder().encode(data.toString().getBytes(StandardCharsets.ISO_8859_1));
    return new String(encodedBytes, StandardCharsets.ISO_8859_1);
  }

}
