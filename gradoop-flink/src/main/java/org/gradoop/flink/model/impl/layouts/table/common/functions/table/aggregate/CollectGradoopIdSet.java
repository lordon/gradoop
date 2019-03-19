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
package org.gradoop.flink.model.impl.layouts.table.common.functions.table.aggregate;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Aggregation function to collect
 * a set of {@link GradoopId} and accumulate them in a {@link GradoopIdSet}
 */
public class CollectGradoopIdSet extends AggregateFunction<GradoopIdSet, GradoopIdSet> {

  @Override
  public GradoopIdSet createAccumulator() {
    return new GradoopIdSet();
  }

  @Override
  public GradoopIdSet getValue(GradoopIdSet gradoopIds) {
    return gradoopIds;
  }

  /**
   * Processes the input gradoop id and update the provided gradoop id set instance.
   *
   * @param accumulator  the gradoop id set which contains the current aggregated results
   * @param value        the input gradoop id
   */
  public void accumulate(GradoopIdSet accumulator, GradoopId value) {
    accumulator.add(value);
  }

  /**
   * Merges a group of property value instances into one property value.
   *
   * @param accumulator  the gradoop id set which will keep the merged aggregate results.
   * @param its          an [[java.lang.Iterable]] pointed to a group of gradoop id sets
   */
  public void merge(GradoopIdSet accumulator, Iterable<GradoopIdSet> its) {
    for (GradoopIdSet set : its) {
      accumulator.addAll(set);
    }
  }

  /**
   * Resets the gradoop id set.
   *
   * @param accumulator  the gradoop id set which needs to be reset
   */
  public void resetAccumulator(GradoopIdSet accumulator) {
    accumulator.clear();
  }

  @Override
  public TypeInformation getResultType() {
    return TypeInformation.of(GradoopIdSet.class);
  }

}
