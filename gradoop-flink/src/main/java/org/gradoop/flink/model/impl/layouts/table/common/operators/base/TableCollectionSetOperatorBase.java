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
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.model.impl.layouts.table.BaseTableSetFactory;
import org.gradoop.flink.model.impl.layouts.table.gve.GVETableSet;

/**
 * Base class for table based graph collection set operator implementations, like Difference,
 * Intersection and Union
 *
 * @param <TS> table set type
 * @param <TSF> table set factory type
 */
public abstract class TableCollectionSetOperatorBase<
  TS extends GVETableSet,
  TSF extends BaseTableSetFactory>
  extends TableBinaryCollectionToCollectionOperatorBase<TS, TSF> {

  @Override
  protected TableGraphCollection computeNewGraphCollection() {
    Table newGraphIds = computeNewGraphIds();
    return config.getTableGraphCollectionFactory().fromTableSet(buildInducedTableSet(newGraphIds));
  }

  /**
   * Computes new set of graph ids based on graph sets of {@link this#firstTableSet} and
   * {@link this#otherTableSet}
   *
   * @return table of new graph ids
   */
  protected abstract Table computeNewGraphIds();

  /**
   * Computes new table set of result graph collection based on the newly computed graph ids
   *
   * @param newGraphIds table of newly computed graph ids
   * @return table set of result graph collection
   */
  protected abstract TS buildInducedTableSet(Table newGraphIds);

}
