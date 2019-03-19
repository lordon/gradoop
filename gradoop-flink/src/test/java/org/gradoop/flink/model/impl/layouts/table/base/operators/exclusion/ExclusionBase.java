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
package org.gradoop.flink.model.impl.layouts.table.base.operators.exclusion;

import org.gradoop.flink.model.TableGradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.util.TableFlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public abstract class ExclusionBase extends TableGradoopFlinkTestBase {

  @Before
  public abstract void setFactories();

  @Test
  public void testSameGraph() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[]");

    TableLogicalGraph tg0 = loader.getTableLogicalGraphByVariable("g0");
    TableLogicalGraph tOverlap = tg0.exclude(tg0);

    LogicalGraph exclusion = fromTableLogicalGraph(tOverlap);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    assertTrue("exclusion of same graph failed",
      expected.equalsByElementIds(exclusion).collect().get(0));
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("" +
      "expected1[(eve)]" +
      "expected2[(carol)-[ckd]->(dave)-[dkc]->(carol)]");

    TableLogicalGraph g0 = loader.getTableLogicalGraphByVariable("g0");
    TableLogicalGraph g2 = loader.getTableLogicalGraphByVariable("g2");
    TableLogicalGraph tg0g2 = g0.exclude(g2);
    TableLogicalGraph tg2g0 = g2.exclude(g0);

    LogicalGraph expected1 = loader.getLogicalGraphByVariable("expected1");
    LogicalGraph expected2 = loader.getLogicalGraphByVariable("expected2");
    LogicalGraph g0g2 = fromTableLogicalGraph(tg0g2);
    LogicalGraph g2g0 = fromTableLogicalGraph(tg2g0);

    assertTrue("excluding overlapping graphs failed",
      expected1.equalsByData(g0g2).collect().get(0));
    assertTrue("excluding switched overlapping graphs failed",
      expected2.equalsByData(g2g0).collect().get(0));
  }

  @Test
  public void testDerivedOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("g[(a {x: true, y: true}),(b {x:true, y: false})]");

    LogicalGraph baseGraph = loader.getLogicalGraphByVariable("g");
    LogicalGraph derivedGraph1 = baseGraph.vertexInducedSubgraph(v -> v.getPropertyValue("x").getBoolean());
    LogicalGraph derivedGraph2 = baseGraph.vertexInducedSubgraph(v -> !v.getPropertyValue("y").getBoolean());

    TableLogicalGraph tDerivedGraph1 = fromDataSetLogicalGraph(derivedGraph1);
    TableLogicalGraph tDerivedGraph2 = fromDataSetLogicalGraph(derivedGraph2);
    TableLogicalGraph tResult = tDerivedGraph1.exclude(tDerivedGraph2);

    loader.appendToDatabaseFromString("expected[(a)]");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph result = fromTableLogicalGraph(tResult);

    collectAndAssertTrue(result.equalsByElementIds(expected));
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph g0 = loader.getLogicalGraphByVariable("g0");
    LogicalGraph g1 = loader.getLogicalGraphByVariable("g1");

    TableLogicalGraph tg0 = fromDataSetLogicalGraph(g0);
    TableLogicalGraph tg1 = fromDataSetLogicalGraph(g1);
    TableLogicalGraph tg0g1 = tg0.exclude(tg1);
    TableLogicalGraph tg1g0 = tg1.exclude(tg0);

    LogicalGraph g0g1 = fromTableLogicalGraph(tg0g1);
    LogicalGraph g1g0 = fromTableLogicalGraph(tg1g0);

    assertTrue("excluding non overlapping graphs failed",
      g0.equalsByElementIds(g0g1).collect().get(0));
    assertTrue("excluding switched non overlapping graphs failed",
      g1.equalsByElementIds(g1g0).collect().get(0));
  }

  @Test
  public void testDerivedNonOverlappingGraphs() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("g[(a {x: true}),(b {x: false})]");

    LogicalGraph baseGraph = loader.getLogicalGraphByVariable("g");
    LogicalGraph derivedGraph1 = baseGraph.vertexInducedSubgraph(v -> v.getPropertyValue("x").getBoolean());
    LogicalGraph derivedGraph2 = baseGraph.vertexInducedSubgraph(v -> !v.getPropertyValue("x").getBoolean());

    TableLogicalGraph tDerivedGraph1 = fromDataSetLogicalGraph(derivedGraph1);
    TableLogicalGraph tDerivedGraph2 = fromDataSetLogicalGraph(derivedGraph2);
    TableLogicalGraph tResult = tDerivedGraph1.exclude(tDerivedGraph2);

    loader.appendToDatabaseFromString("expected[(a)]");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph result = fromTableLogicalGraph(tResult);

    collectAndAssertTrue(result.equalsByElementIds(expected));
  }

}
