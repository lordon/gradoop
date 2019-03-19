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
package org.gradoop.flink.model.impl.layouts.table.base.operators.overlap;

import org.gradoop.flink.model.TableGradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.util.TableFlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public abstract class OverlapBase extends TableGradoopFlinkTestBase {

  @Before
  public abstract void setFactories();

  @Test
  public void testSameGraph() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph tGraph = loader.getTableLogicalGraphByVariable("g0");
    TableLogicalGraph tOverlap = tGraph.overlap(tGraph);

    LogicalGraph graph = fromTableLogicalGraph(tGraph);
    LogicalGraph overlap = fromTableLogicalGraph(tOverlap);

    assertTrue("overlap of same graph failed",
      graph.equalsByElementIds(overlap).collect().get(0));
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString(
      "expected[(alice)-[akb]->(bob)-[bka]->(alice)]"
    );

    TableLogicalGraph g0 = loader.getTableLogicalGraphByVariable("g0");
    TableLogicalGraph g2 = loader.getTableLogicalGraphByVariable("g2");

    TableLogicalGraph tg0g2 = g0.overlap(g2);
    TableLogicalGraph tg2g0 = g2.overlap(g0);

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
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("g[(a)]");

    LogicalGraph baseGraph = loader.getLogicalGraphByVariable("g");
    LogicalGraph derivedGraph1 = baseGraph.vertexInducedSubgraph(v -> true);
    LogicalGraph derivedGraph2 = baseGraph.vertexInducedSubgraph(v -> true);

    TableLogicalGraph tDerivedGraph1 = fromDataSetLogicalGraph(derivedGraph1);
    TableLogicalGraph tDerivedGraph2 = fromDataSetLogicalGraph(derivedGraph2);
    TableLogicalGraph tResult = tDerivedGraph1.overlap(tDerivedGraph2);

    loader.appendToDatabaseFromString("expected[(a)]");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph result = fromTableLogicalGraph(tResult);

    collectAndAssertTrue(result.equalsByElementIds(expected));
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[]");

    TableLogicalGraph g0 = loader.getTableLogicalGraphByVariable("g0");
    TableLogicalGraph g1 = loader.getTableLogicalGraphByVariable("g1");
    TableLogicalGraph tg0g1 = g0.overlap(g1);
    TableLogicalGraph tg1g0 = g1.overlap(g0);

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph g0g1 = fromTableLogicalGraph(tg0g1);
    LogicalGraph g1g0 = fromTableLogicalGraph(tg1g0);

    assertTrue("overlap non overlapping graphs failed",
      expected.equalsByElementIds(g0g1).collect().get(0));
    assertTrue("overlap switched non overlapping graphs failed",
      expected.equalsByElementIds(g1g0).collect().get(0));
  }

  @Test
  public void testDerivedNonOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("g[(a {x: true}),(b {x: false})]");

    LogicalGraph baseGraph = loader.getLogicalGraphByVariable("g");
    LogicalGraph derivedGraph1 = baseGraph.vertexInducedSubgraph(v -> v.getPropertyValue("x").getBoolean());
    LogicalGraph derivedGraph2 = baseGraph.vertexInducedSubgraph(v -> !v.getPropertyValue("x").getBoolean());

    TableLogicalGraph tDerivedGraph1 = fromDataSetLogicalGraph(derivedGraph1);
    TableLogicalGraph tDerivedGraph2 = fromDataSetLogicalGraph(derivedGraph2);
    TableLogicalGraph tResult = tDerivedGraph1.overlap(tDerivedGraph2);

    loader.appendToDatabaseFromString("expected[]");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph result = fromTableLogicalGraph(tResult);

    collectAndAssertTrue(result.equalsByElementIds(expected));
  }

  @Test
  public void testVertexOnlyOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString(
      "g1[(a)-[e1]->(b)]" +
        "g2[(a)-[e2]->(b)]" +
        "expected[(a)(b)]");
    TableLogicalGraph g1 = loader.getTableLogicalGraphByVariable("g1");
    TableLogicalGraph g2 = loader.getTableLogicalGraphByVariable("g2");
    TableLogicalGraph tResult = g1.overlap(g2);

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph result = fromTableLogicalGraph(tResult);

    collectAndAssertTrue(result.equalsByElementIds(expected));
  }

}
