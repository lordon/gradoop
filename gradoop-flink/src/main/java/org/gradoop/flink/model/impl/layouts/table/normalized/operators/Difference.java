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
package org.gradoop.flink.model.impl.layouts.table.normalized.operators;

import org.apache.flink.table.api.Table;
import org.gradoop.flink.model.impl.layouts.table.gve.operators.base.GVEOperatorUtils;
import org.gradoop.flink.model.impl.layouts.table.normalized.operators.base.NormalizedCollectionSetOperatorBase;

/**
 * Returns a collection with all logical graphs that are contained in the first input collection
 * but not in the second.
 * Graph equality is based on their respective identifiers.
 * Implemented in normalized layout.
 */
public class Difference extends NormalizedCollectionSetOperatorBase {

  @Override
  protected Table computeNewGraphIds() {
    return GVEOperatorUtils.graphIdDifference(firstTableSet, otherTableSet);
  }

}