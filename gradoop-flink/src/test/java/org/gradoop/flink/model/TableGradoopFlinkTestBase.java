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
package org.gradoop.flink.model;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.util.TableFlinkAsciiGraphLoader;

import java.io.IOException;
import java.io.InputStream;

public abstract class TableGradoopFlinkTestBase extends GradoopFlinkTestBase {

  protected TableFlinkAsciiGraphLoader getLoaderFromString(String asciiString) {
    TableFlinkAsciiGraphLoader loader = getNewLoader();
    loader.initDatabaseFromString(asciiString);
    return loader;
  }

  protected TableFlinkAsciiGraphLoader getLoaderFromStream(InputStream inputStream) throws IOException {
    TableFlinkAsciiGraphLoader loader = getNewLoader();

    loader.initDatabaseFromStream(inputStream);
    return loader;
  }

  /**
   * Creates a social network as a basis for tests.
   * <p/>
   * An image of the network can be found in
   * gradoop/dev-support/social-network.pdf
   *
   * @return graph store containing a simple social network for tests.
   */
  protected TableFlinkAsciiGraphLoader getSocialNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);
    return getLoaderFromStream(inputStream);
  }

  /**
   * Returns an uninitialized loader with the test config.
   *
   * @return uninitialized Flink Ascii graph loader
   */
  private TableFlinkAsciiGraphLoader getNewLoader() {
    return new TableFlinkAsciiGraphLoader(getConfig());
  }

  protected TableLogicalGraph fromDataSetLogicalGraph(LogicalGraph lg) {
    return getConfig().getTableLogicalGraphFactory().fromDataSets(lg.getGraphHead(),
      lg.getVertices(), lg.getEdges());
  }

  protected LogicalGraph fromTableLogicalGraph(TableLogicalGraph lg) {
    return getConfig().getLogicalGraphFactory().fromTableLogicalGraph(lg);
  }

}
