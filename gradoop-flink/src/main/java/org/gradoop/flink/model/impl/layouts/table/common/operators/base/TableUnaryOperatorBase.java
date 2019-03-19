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

import org.apache.flink.table.api.Table;
import org.gradoop.flink.model.api.layouts.table.TableLayout;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;

/**
 * Base class for all unary operators in table layouts based on GVE layout.
 *
 * @param <TS> table set type
 * @param <TSF> table set factory type
 */
public abstract class TableUnaryOperatorBase<
  TS extends GVETableSet,
  TSF extends BaseTableSetFactory> extends TableOperatorBase<TS, TSF> {

  /**
   * Table set of the original logical graph or graph collection
   */
  protected TS tableSet;

  /**
   * Computes vertex induced edges
   * @see TableOperatorBase#computeNewVertexInducedEdges(GVETableSet, Table, Table)
   *
   * @param originalEdges original edges table
   * @param inducingVertices inducing vertices table
   * @return vertex induced edges table
   */
  protected Table computeNewVertexInducedEdges(Table originalEdges,
    Table inducingVertices) {
    return super.computeNewVertexInducedEdges(this.tableSet, originalEdges, inducingVertices);
  }

  /**
   * Computes edge induced vertices
   * @see TableOperatorBase#computeNewEdgeInducedVertices(GVETableSet, Table, Table)
   *
   * @param originalVertices original vertices table
   * @param inducingEdges inducing edges table
   * @return edge induced vertices table
   */
  protected Table computeNewEdgeInducedVertices(Table originalVertices,
    Table inducingEdges) {
    return super.computeNewEdgeInducedVertices(this.tableSet, originalVertices, inducingEdges);
  }

  @Override
  protected void registerTools(TableLayout tableLayout) {
    super.registerTools(tableLayout);
    this.tableSet = (TS) tableLayout.getTableSet();
  }
}
