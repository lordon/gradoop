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
package org.gradoop.flink.model.impl.layouts.table.base.operators.subgraph;

import org.gradoop.flink.model.TableGradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.util.TableFlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

public abstract class SubgraphBase extends TableGradoopFlinkTestBase {

  @Before
  public abstract void setFactories();

  @Test
  public void testExistingSubgraph() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave)" +
      "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave)" +
      "(eve)-[eka]->(alice)" +
      "(eve)-[ekb]->(bob)" +
      "(frank)-[fkc]->(carol)" +
      "(frank)-[fkd]->(dave)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraph();
    TableLogicalGraph tOutput = input.subgraph(
      new String[] { "Person" },
      new String[] { "knows" }
    );

    LogicalGraph output = fromTableLogicalGraph(tOutput);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testExistingSubgraphWithVerification() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave)" +
      "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave)" +
      "(eve)-[eka]->(alice)" +
      "(eve)-[ekb]->(bob)" +
      "(frank)-[fkc]->(carol)" +
      "(frank)-[fkd]->(dave)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraph();
    TableLogicalGraph tOuput = input.subgraph(
      new String[] { "Person"},
      new String[] { "knows" },
      Subgraph.Strategy.BOTH_VERIFIED
    );

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph output = fromTableLogicalGraph(tOuput);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  /**
   * Extracts a subgraph where only vertices fulfill the filter function.
   */
  @Test
  public void testPartialSubgraph() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice),(bob),(carol),(dave),(eve),(frank)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraph();
    TableLogicalGraph tOuput = input.subgraph(
      new String[] { "Person"},
      new String[] { "friendOf" }
    );

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph output = fromTableLogicalGraph(tOuput);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  /**
   * Extracts a subgraph which is empty.
   *
   * @throws Exception
   */
  @Test
  public void testEmptySubgraph() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[]");

    TableLogicalGraph input = loader.getTableLogicalGraph();
    TableLogicalGraph tOuput = input.subgraph(
      new String[] { "User"},
      new String[] { "friendOf" }
    );

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph output = fromTableLogicalGraph(tOuput);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }


  @Test
  public void testVertexInducedSubgraph() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraph();
    TableLogicalGraph tOutput = input.vertexInducedSubgraph("Forum", "Tag");

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

  @Test
  public void testEdgeInducedSubgraph() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
      "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraph();
    TableLogicalGraph tOutput = input.edgeInducedSubgraph("hasTag");

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }

}
