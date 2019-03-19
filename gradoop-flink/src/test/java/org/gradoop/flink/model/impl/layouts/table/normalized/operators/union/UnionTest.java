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
package org.gradoop.flink.model.impl.layouts.table.normalized.operators.union;

import org.gradoop.flink.model.impl.layouts.table.base.operators.union.UnionBase;
import org.gradoop.flink.model.impl.layouts.table.normalized.TableNormalizedGraphCollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.table.normalized.TableNormalizedLogicalGraphLayoutFactory;

public class UnionTest extends UnionBase {

  @Override
  public void setFactories() {
    getConfig().setTableLogicalGraphLayoutFactory(new TableNormalizedLogicalGraphLayoutFactory());
    getConfig().setTableGraphCollectionLayoutFactory(new TableNormalizedGraphCollectionLayoutFactory());
  }

}
