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
package org.gradoop.flink.model.api.layouts.table.operators;

import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayout;
import org.gradoop.flink.model.api.operators.Operator;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;

/**
 * Creates a {@link TableLogicalGraph} based on two input graphs.
 */
public interface TableBinaryGraphToGraphOperator extends Operator {

  /**
   * Executes the operator.
   *
   * @param firstGraphLayout first input graph
   * @param secondGraphLayout second input graph
   * @return operator result
   */
  TableLogicalGraph execute(TableLogicalGraphLayout firstGraphLayout,
    TableLogicalGraphLayout secondGraphLayout);
}
