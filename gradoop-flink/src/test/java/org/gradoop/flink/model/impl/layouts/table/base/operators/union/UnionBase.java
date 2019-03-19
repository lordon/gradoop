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
package org.gradoop.flink.model.impl.layouts.table.base.operators.union;

import org.gradoop.flink.model.TableGradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.table.TableGraphCollection;
import org.gradoop.flink.util.TableFlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public abstract class UnionBase extends TableGradoopFlinkTestBase {

  @Before
  public abstract void setFactories();

  @Test
  public void testOverlappingCollections() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableGraphCollection col02 = loader.getTableGraphCollectionByVariables("g0", "g2");

    TableGraphCollection col12 = loader.getTableGraphCollectionByVariables("g1", "g2");

    TableGraphCollection result = col02.union(col12);

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    checkAssertions(expectation, result, "");
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableGraphCollection col01 = loader.getTableGraphCollectionByVariables("g0", "g1");

    TableGraphCollection col23 = loader.getTableGraphCollectionByVariables("g2", "g3");

    TableGraphCollection result = col01.union(col23);

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    checkAssertions(expectation, result, "non");
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("g0", "g1");

    TableGraphCollection tExpectation =
      getConfig().getTableGraphCollectionFactory().fromDataSets(expectation.getGraphHeads(),
        expectation.getVertices(), expectation.getEdges());

    TableGraphCollection result = tExpectation.union(tExpectation);

    checkAssertions(expectation, result, "total");
  }

  protected void checkAssertions(GraphCollection expectation,
    TableGraphCollection tResult, String attribute) throws Exception {
    GraphCollection result = getConfig().getGraphCollectionFactory()
      .fromTableGraphCollection(tResult);
    assertTrue(
      "wrong graph ids for " + attribute + " overlapping collections",
      result.equalsByGraphIds(expectation).collect().get(0));
    assertTrue(
      "wrong graph element ids for" + attribute + " overlapping collections",
      result.equalsByGraphElementData(expectation).collect().get(0));
  }

}
