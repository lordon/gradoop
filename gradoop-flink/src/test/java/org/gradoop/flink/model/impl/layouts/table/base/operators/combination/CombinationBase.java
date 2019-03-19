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
package org.gradoop.flink.model.impl.layouts.table.base.operators.combination;

import org.gradoop.flink.model.TableGradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.util.TableFlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public abstract class CombinationBase extends TableGradoopFlinkTestBase {

  @Before
  public abstract void setFactories();

  @Test
  public void testSameGraph() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph tg0 = loader.getTableLogicalGraphByVariable("g0");
    TableLogicalGraph tCombination = tg0.combine(tg0);

    LogicalGraph g0 = fromTableLogicalGraph(tg0);
    LogicalGraph combination = fromTableLogicalGraph(tCombination);

    assertTrue("combining same graph failed",
      g0.equalsByElementIds(combination).collect().get(0));
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob)" +
      "(bob)-[bka]->(alice)" +
      "(bob)-[bkc]->(carol)" +
      "(carol)-[ckb]->(bob)" +
      "(carol)-[ckd]->(dave)" +
      "(dave)-[dkc]->(carol)" +
      "(eve)-[eka]->(alice)" +
      "(eve)-[ekb]->(bob)]"
    );

    TableLogicalGraph g0 = loader.getTableLogicalGraphByVariable("g0");
    TableLogicalGraph g2 = loader.getTableLogicalGraphByVariable("g2");

    TableLogicalGraph tg0g2 = g0.combine(g2);
    TableLogicalGraph tg2g0 = g2.combine(g0);

    LogicalGraph g0g2 = fromTableLogicalGraph(tg0g2);
    LogicalGraph g2g0 = fromTableLogicalGraph(tg2g0);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    assertTrue("combining overlapping graphs failed",
      expected.equalsByElementIds(g0g2).collect().get(0));
    assertTrue("combining switched overlapping graphs failed",
      expected.equalsByElementIds(g2g0).collect().get(0));
  }

  @Test
  public void testDerivedOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("g[(a {x: true, y: true}), (b {x: true, y: false})]");

    LogicalGraph baseGraph = loader.getLogicalGraphByVariable("g");
    LogicalGraph derivedGraph1 = baseGraph.vertexInducedSubgraph(v -> v.getPropertyValue("x").getBoolean());
    LogicalGraph derivedGraph2 = baseGraph.vertexInducedSubgraph(v -> !v.getPropertyValue("y").getBoolean());

    TableLogicalGraph tDerivedGraph1 = fromDataSetLogicalGraph(derivedGraph1);
    TableLogicalGraph tDerivedGraph2 = fromDataSetLogicalGraph(derivedGraph2);
    TableLogicalGraph tResult = tDerivedGraph1.combine(tDerivedGraph2);

    loader.appendToDatabaseFromString("expected[(a),(b)]");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph result = fromTableLogicalGraph(tResult);

    collectAndAssertTrue(result.equalsByElementIds(expected));
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(alice)-[akb]->(bob)" +
      "(bob)-[bka]->(alice)" +
      "(eve)-[eka]->(alice)" +
      "(eve)-[ekb]->(bob)" +
      "(carol)-[ckd]->(dave)" +
      "(dave)-[dkc]->(carol)" +
      "(frank)-[fkc]->(carol)" +
      "(frank)-[fkd]->(dave)]"
    );

    TableLogicalGraph g0 = loader.getTableLogicalGraphByVariable("g0");
    TableLogicalGraph g1 = loader.getTableLogicalGraphByVariable("g1");
    TableLogicalGraph tg0g1 = g0.combine(g1);
    TableLogicalGraph tg1g0 = g1.combine(g0);

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph g0g1 = fromTableLogicalGraph(tg0g1);
    LogicalGraph g1g0 = fromTableLogicalGraph(tg1g0);

    assertTrue("combining non overlapping graphs failed",
      expected.equalsByElementIds(g0g1).collect().get(0));
    assertTrue("combining switched non overlapping graphs failed",
      expected.equalsByElementIds(g1g0).collect().get(0));
  }

  @Test
  public void testDerivedNonOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("g[(a {x: true}), (b {x: false})]");

    LogicalGraph baseGraph = loader.getLogicalGraphByVariable("g");
    LogicalGraph derivedGraph1 = baseGraph.vertexInducedSubgraph(v -> v.getPropertyValue("x").getBoolean());
    LogicalGraph derivedGraph2 = baseGraph.vertexInducedSubgraph(v -> !v.getPropertyValue("x").getBoolean());

    TableLogicalGraph tDerivedGraph1 = fromDataSetLogicalGraph(derivedGraph1);
    TableLogicalGraph tDerivedGraph2 = fromDataSetLogicalGraph(derivedGraph2);
    TableLogicalGraph tResult = tDerivedGraph1.combine(tDerivedGraph2);

    loader.appendToDatabaseFromString("expected[(a),(b)]");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph result = fromTableLogicalGraph(tResult);

    collectAndAssertTrue(result.equalsByElementIds(expected));
  }

}
