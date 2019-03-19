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
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;

/**
 * Takes Base64 encoded string and returns decoded properties object
 */
public class ParseProperties extends ScalarFunction {

  /**
   * Returns decoded properties from given Base64 string. The decoded properties object gets
   * build in the following way:
   *
   * - Decoded Base64 string is a JSON string
   * - JSON keys become Property keys
   * - JSON values are Base64 encoded strings and get decoded to property values
   * - Property keys and values get collected into a properties object
   *
   * @param s Base64 encoded string
   * @return decoded properties
   * @throws JSONException
   */
  public Properties eval(String s) throws JSONException {
    String json = new String(Base64.getDecoder().decode(s), StandardCharsets.ISO_8859_1);
    JSONObject jsonProperties = new JSONObject(json);

    Properties props = Properties.createWithCapacity(jsonProperties.length() * 2);

    Iterator<?> keys = jsonProperties.keys();
    while (keys.hasNext()) {
      String key = keys.next().toString();
      byte[] base64decodedBytes = Base64.getDecoder().decode(jsonProperties.getString(key));
      PropertyValue pv = PropertyValue.fromRawBytes(base64decodedBytes);
      props.set(key, pv);
    }
    return props;
  }

}
