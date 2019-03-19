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
package org.gradoop.flink.model.impl.layouts.table.common.functions.table.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.TableFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Collects each gradoop id of a gradoop id set
 */
public class ExpandGradoopIdSet extends TableFunction<GradoopId> {

  /**
   * Collects each gradoop id of a gradoop id set
   *
   * @param set gradoop id set
   */
  public void eval(GradoopIdSet set) {
    for (GradoopId id : set) {
      collect(id);
    }
  }

  @Override
  public TypeInformation<GradoopId> getResultType() {
    return TypeInformation.of(GradoopId.class);
  }
}