/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.pubchem_asn;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.junit.Test;

public class PubchemASNDataSourceTest extends GradoopFlinkTestBase {

  @Test
  public void testRead() throws Exception {
    String asnPath = PubchemASNDataSourceTest.class
            .getResource("/data/pubchem_asn/input.asn")
            .getFile();

    String gdlPath = PubchemASNDataSourceTest.class
            .getResource("/data/pubchem_asn/expected.gdl")
            .getFile();

    DataSource dataSource = new PubchemASNDataSource(asnPath, getConfig());
    GraphCollection input = dataSource.getGraphCollection();
    GraphCollection expected = getLoaderFromFile(gdlPath).
            getGraphCollectionByVariables("g0", "g1");

    collectAndAssertTrue(input.equalsByGraphData(expected));
  }

}
