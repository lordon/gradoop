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
package org.gradoop.flink.io.api.table;

import org.gradoop.flink.model.api.layouts.table.TableGraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayout;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;

import java.io.IOException;

/**
 * Data source in analytical programs.
 */
public interface TableDataSink {

  /**
   * Writes a logical graph to the data sink.
   *
   * @param logicalGraph logical graph
   */
  void write(TableLogicalGraph logicalGraph) throws IOException;

  /**
   * Writes a graph collection graph to the data sink.
   *
   * @param graphCollection graph collection
   */
  void write(TableGraphCollection graphCollection) throws IOException;

  /**
   * Writes a logical graph to the data sink with overwrite option.
   *
   * @param logicalGraph logical graph
   * @param overwrite    true, if existing files should be overwritten
   */
  void write(TableLogicalGraph logicalGraph, boolean overwrite) throws IOException;

  /**
   * Writes a graph collection to the data sink with overwrite option.
   *
   * @param graphCollection graph collection
   * @param overwrite       true, if existing files should be overwritten
   */
  void write(TableGraphCollection graphCollection, boolean overwrite) throws IOException;

  /**
   * Writes a logical graph layout to the data sink.
   *
   * @param logicalGraphLayout logical graph layout
   */
  void write(TableLogicalGraphLayout logicalGraphLayout) throws IOException;

  /**
   * Writes a graph collection layout to the data sink.
   *
   * @param graphCollectionLayout graph collection layout
   */
  void write(TableGraphCollectionLayout graphCollectionLayout) throws IOException;

  /**
   * Writes a logical graph layout to the data sink with overwrite option.
   *
   * @param logicalGraphLayout logical graph layout
   * @param overwrite          true, if existing files should be overwritten
   */
  void write(TableLogicalGraphLayout logicalGraphLayout, boolean overwrite) throws IOException;

  /**
   * Writes a graph collection layout to the data sink with overwrite option.
   *
   * @param graphCollectionLayout graph collection layout
   * @param overwrite             true, if existing files should be overwritten
   */
  void write(TableGraphCollectionLayout graphCollectionLayout, boolean overwrite)
    throws IOException;
}
