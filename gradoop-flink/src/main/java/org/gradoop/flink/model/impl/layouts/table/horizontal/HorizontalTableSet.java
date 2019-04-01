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

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.layouts.table.BaseTableSet;
import org.gradoop.flink.model.impl.layouts.table.TableSet;
import org.gradoop.flink.model.impl.layouts.table.TableSetSchema;
import org.gradoop.flink.model.impl.layouts.table.normalized.NormalizedTableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Horizontal table set. Each table not being defined in initial schema gets treated as
 * property table implicitly.
 */
public class HorizontalTableSet extends NormalizedTableSet implements BaseTableSet {

  /**
   * Initial table set schema map without property table
   */
  private static ImmutableMap<String, TableSchema> INITIAL_SCHEMA =
    ImmutableMap.<String, TableSchema>builder()
    .put(TABLE_VERTICES, new TableSchema.Builder()
      .field(FIELD_VERTEX_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_VERTEX_LABEL, Types.STRING())
      .build()
    )
    .put(TABLE_EDGES, new TableSchema.Builder()
      .field(FIELD_EDGE_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_TAIL_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_HEAD_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_EDGE_LABEL, Types.STRING())
      .build()
    )
    .put(TABLE_GRAPHS, new TableSchema.Builder()
      .field(FIELD_GRAPH_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_GRAPH_LABEL, Types.STRING())
      .build()
    )
    .put(TABLE_VERTICES_GRAPHS, new TableSchema.Builder()
      .field(FIELD_GRAPH_VERTEX_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_VERTEX_GRAPH_ID, TypeInformation.of(GradoopId.class))
      .build()
    )
    .put(TABLE_EDGES_GRAPHS, new TableSchema.Builder()
      .field(FIELD_GRAPH_EDGE_ID, TypeInformation.of(GradoopId.class))
      .field(FIELD_EDGE_GRAPH_ID, TypeInformation.of(GradoopId.class))
      .build()
    )
    .build();

  /**
   * Table set schema
   */
  static final TableSetSchema SCHEMA = new TableSetSchema(INITIAL_SCHEMA);

  /**
   * Constructor
   */
  public HorizontalTableSet() {
  }

  /**
   * Takes a generic {@link TableSet} as parameter and adds all tables of given table set to the
   * new one
   *
   * @param tableSet existing table set
   */
  public HorizontalTableSet(TableSet tableSet) {
    super(tableSet);
  }

  @Override
  public TableSetSchema getSchema() {
    return SCHEMA;
  }

  /**
   * Returns a list of all property tables. Each table of table set which is not defined in
   * initial schema gets treated as property table.
   *
   * @return list of all property tables
   */
  public List<PropertyTable> getPropertyTables() {
    List<PropertyTable> propertyTables = new ArrayList<>();
    for (Map.Entry<String, Table> keyTablePair : this.entrySet()) {
      if (!INITIAL_SCHEMA.keySet().contains(keyTablePair.getKey())) {
        propertyTables.add(new PropertyTable(keyTablePair.getKey(), keyTablePair.getValue()));
      }
    }
    return propertyTables;
  }

}
