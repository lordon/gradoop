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
package org.gradoop.flink.model.impl.layouts.table.horizontal;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.layouts.table.util.TableUtils;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Wraps a flink table together with corresponding property key.
 */
public class PropertyTable {

  /**
   * Field name template for element id
   */
  private static String FIELD_ELEMENT_ID_TEMPLATE = "%s_element_id";
  /**
   * Field name template for property value
   */
  private static String FIELD_PROPERTY_VALUE_TEMPLATE = "%s_property_value";

  /**
   * Property key
   */
  private String propertyKey;
  /**
   * Table containing property values
   */
  private Table table;
  /**
   * Schema of table
   */
  private TableSchema schema;

  /**
   * Constructor
   *
   * @param propertyKey property key
   * @param table table containing property values
   */
  public PropertyTable(String propertyKey, Table table) {
    this.propertyKey = propertyKey;
    this.table = table;
    this.schema = buildPropertyTableSchema(propertyKey);
  }

  /**
   * Returns property key
   *
   * @return property key
   */
  public String getPropertyKey() {
    return propertyKey;
  }

  /**
   * Returns table containing property values
   *
   * @return table containing property values
   */
  public Table getTable() {
    return table;
  }

  /**
   * Overwrites table containing property values
   *
   * @param table table containing property values
   */
  public void setTable(Table table) {
    this.table = table;
  }

  /**
   * Returns table schema
   *
   * @return table schema
   */
  public TableSchema getSchema() {
    return schema;
  }

  /**
   * Return field name for element id
   *
   * @return field name for element id
   */
  public String getElementIdFieldName() {
    return String.format(FIELD_ELEMENT_ID_TEMPLATE, this.propertyKey);
  }

  /**
   * Return field name for property value
   *
   * @return field name for property value
   */
  public String getPropertyValueFieldName() {
    return String.format(FIELD_PROPERTY_VALUE_TEMPLATE, this.propertyKey);
  }

  /**
   * Builds {@link TableSchema} for given property key
   *
   * @param propertyKey property key
   * @return table schema
   */
  public static TableSchema buildPropertyTableSchema(String propertyKey) {
    return new TableSchemaBuilder()
      .field(String.format(FIELD_ELEMENT_ID_TEMPLATE, propertyKey),
        TypeInformation.of(GradoopId.class))
      .field(String.format(FIELD_PROPERTY_VALUE_TEMPLATE, propertyKey),
        TypeInformation.of(PropertyValue.class))
      .build();
  }

  /**
   * Returns an instance of PropertyTable for given property with an empty table.
   *
   * @param propertyKey property key
   * @param config current gradoop configuration
   * @return instance of PropertyTable for given property with an empty table
   */
  public static PropertyTable createEmptyPropertyTable(String propertyKey,
    GradoopFlinkConfig config) {
    return new PropertyTable(propertyKey,
      TableUtils.createEmptyTable(config, buildPropertyTableSchema(propertyKey)));
  }
}
