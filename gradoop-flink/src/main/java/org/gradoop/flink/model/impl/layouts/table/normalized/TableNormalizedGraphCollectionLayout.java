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
package org.gradoop.flink.model.impl.layouts.table.normalized;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.io.api.table.TableDataSink;
import org.gradoop.flink.model.api.layouts.table.TableGraphCollectionLayout;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.layouts.table.normalized.operators.Difference;
import org.gradoop.flink.model.impl.layouts.table.normalized.operators.Intersection;
import org.gradoop.flink.model.impl.layouts.table.normalized.operators.Union;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Normalized layout of {@link TableGraphCollection}
 */
public class TableNormalizedGraphCollectionLayout extends TableNormalizedLayout implements
  TableGraphCollectionLayout {

  /**
   * Constructor
   *
   * @param tableSet normalized table set
   * @param config gradoop configuration
   */
  TableNormalizedGraphCollectionLayout(NormalizedTableSet tableSet, GradoopFlinkConfig config) {
    super(tableSet, config);
  }

  @Override
  public TableLogicalGraph getGraph(GradoopId graphID) {
    return null;
  }

  @Override
  public TableGraphCollection getGraphs(GradoopId... identifiers) {
    return null;
  }

  @Override
  public TableGraphCollection getGraphs(GradoopIdSet identifiers) {
    return null;
  }

  @Override
  public TableGraphCollection union(TableGraphCollection otherCollection) {
    return new Union().execute(this, otherCollection.getLayout());
  }

  @Override
  public TableGraphCollection intersect(TableGraphCollection otherCollection) {
    return new Intersection().execute(this, otherCollection.getLayout());
  }

  @Override
  public TableGraphCollection intersectWithSmallResult(TableGraphCollection otherCollection) {
    return new Intersection().execute(this, otherCollection.getLayout());
  }

  @Override
  public TableGraphCollection difference(TableGraphCollection otherCollection) {
    return new Difference().execute(this, otherCollection.getLayout());
  }

  @Override
  public TableGraphCollection differenceWithSmallResult(TableGraphCollection otherCollection) {
    return new Difference().execute(this, otherCollection.getLayout());
  }

  @Override
  public void writeTo(TableDataSink dataSink) throws IOException {
    dataSink.write(this);
  }

  @Override
  public void writeTo(TableDataSink dataSink, boolean overWrite) throws IOException {
    dataSink.write(this, overWrite);
  }

  @Override
  public DataSet<Boolean> isEmpty() {
    throw new NotImplementedException();
  }
}
