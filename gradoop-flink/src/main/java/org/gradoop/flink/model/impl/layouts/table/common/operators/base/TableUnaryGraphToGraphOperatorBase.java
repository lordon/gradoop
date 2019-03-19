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
package org.gradoop.flink.model.impl.layouts.table.common.operators.base;

import org.gradoop.flink.model.api.layouts.table.TableLogicalGraphLayout;
import org.gradoop.flink.model.api.layouts.table.operators.TableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;

/**
 * Base class for operators implementing {@link TableUnaryGraphToGraphOperator}
 *
 * @param <TS> table set type
 * @param <TSF> table set factory type
 */
public abstract class TableUnaryGraphToGraphOperatorBase<
  TS extends GVETableSet,
  TSF extends BaseTableSetFactory> extends TableUnaryOperatorBase<TS, TSF>
  implements TableUnaryGraphToGraphOperator {

  @Override
  public TableLogicalGraph execute(TableLogicalGraphLayout graphLayout) {
    registerTools(graphLayout);
    return computeNewLogicalGraph();
  }

  /**
   * Performs operator logic based on {@link this#tableSet}
   *
   * @return operator output logical graph
   */
  protected abstract TableLogicalGraph computeNewLogicalGraph();
}
