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
package org.gradoop.flink.model.impl.layouts.table.horizontal.operators;

import org.apache.flink.table.api.Table;
import org.gradoop.flink.model.impl.layouts.table.gve.operators.base.GVEOperatorUtils;
import org.gradoop.flink.model.impl.layouts.table.horizontal.operators.base.HorizontalGraphSetOperatorBase;

/**
 * Computes the exclusion graph from two logical graphs. Implemented in horizontal layout.
 */
public class Exclusion extends HorizontalGraphSetOperatorBase {

  @Override
  protected Table computeNewVertices() {
    return GVEOperatorUtils.excludeVerticesById(tableEnv, firstTableSet, otherTableSet);
  }

  @Override
  protected Table computeNewEdges(Table newVertices) {
    return computeNewVertexInducedEdges(firstTableSet.getEdges(), newVertices);
  }

}
