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

import org.gradoop.flink.model.api.layouts.table.TableGraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.table.operators.TableBinaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;

/**
 * Base class for all operators implementing {@link TableBinaryCollectionToCollectionOperator}
 *
 * @param <TS> table set type
 * @param <TSF> table set factory type
 */
public abstract class TableBinaryCollectionToCollectionOperatorBase<
  TS extends GVETableSet,
  TSF extends BaseTableSetFactory> extends TableBinaryOperatorBase<TS, TSF>
  implements TableBinaryCollectionToCollectionOperator {

  @Override
  public TableGraphCollection execute(TableGraphCollectionLayout firstCollectionLayout,
    TableGraphCollectionLayout secondCollectionLayout) {
    registerTools(firstCollectionLayout, secondCollectionLayout);
    return computeNewGraphCollection();
  }

  /**
   * Performs operator logic based on {@link this#firstTableSet} and {@link this#otherTableSet}
   *
   * @return operator output graph collection
   */
  protected abstract TableGraphCollection computeNewGraphCollection();

}
