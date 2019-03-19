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
package org.gradoop.flink.model.impl.layouts.table.base.operators.grouping;

import org.gradoop.flink.model.TableGradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.table.TableLogicalGraph;
import org.gradoop.flink.model.impl.layouts.table.common.operators.grouping.TableGroupingBuilderBase;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.util.TableFlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.gradoop.common.util.GradoopConstants.NULL_STRING;

public abstract class GroupingBase extends TableGradoopFlinkTestBase {

  @Before
  public abstract void setFactories();

  public abstract TableGroupingBuilderBase getGroupingBuilder();

  @Test
  public void testAPIFunction() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader
      .getTableLogicalGraphByVariable("g0")
      .combine(loader.getTableLogicalGraphByVariable("g1"))
      .combine(loader.getTableLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city : \"Leipzig\", count : 2L})" +
      "(pD:Person {city : \"Dresden\", count : 3L})" +
      "(pB:Person {city : \"Berlin\", count : 1L})" +
      "(pD)-[:knows {since : 2014, count : 2L}]->(pD)" +
      "(pD)-[:knows {since : 2013, count : 2L}]->(pL)" +
      "(pD)-[:knows {since : 2015, count : 1L}]->(pL)" +
      "(pL)-[:knows {since : 2014, count : 2L}]->(pL)" +
      "(pL)-[:knows {since : 2013, count : 1L}]->(pD)" +
      "(pB)-[:knows {since : 2015, count : 2L}]->(pD)" +
      "]");

    TableLogicalGraph tOutput = input.groupBy(
      Arrays.asList(TableGroupingBuilderBase.LABEL_SYMBOL, "city"),
      Collections.singletonList(new Count("count")),
      Arrays.asList(TableGroupingBuilderBase.LABEL_SYMBOL, "since"),
      Collections.singletonList(new Count("count"))
    );

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(expected));
  }

  @Test
  public void testVertexPropertySymmetricGraph() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("g2");

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city : \"Leipzig\", count : 2L})" +
      "(dresden {city : \"Dresden\", count : 2L})" +
      "(leipzig)-[{count : 2L}]->(leipzig)" +
      "(leipzig)-[{count : 1L}]->(dresden)" +
      "(dresden)-[{count : 2L}]->(dresden)" +
      "(dresden)-[{count : 1L}]->(leipzig)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("city")
      .addEdgeAggregateFunction(new Count("count"))
      .addVertexAggregateFunction(new Count("count"))
      .build()
      .execute(input.getLayout());

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(output.equalsByElementData(expected));
  }


  @Test
  public void testSingleVertexProperty() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city : \"Leipzig\", count : 2L})" +
      "(dresden {city : \"Dresden\", count : 3L})" +
      "(berlin  {city : \"Berlin\",  count : 1L})" +
      "(dresden)-[{count : 2L}]->(dresden)" +
      "(dresden)-[{count : 3L}]->(leipzig)" +
      "(leipzig)-[{count : 2L}]->(leipzig)" +
      "(leipzig)-[{count : 1L}]->(dresden)" +
      "(berlin)-[{count : 2L}]->(dresden)" +
      "]");

    TableLogicalGraph tInput = fromDataSetLogicalGraph(input);

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("city")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(tInput.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleVertexProperties() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    loader.appendToDatabaseFromString("expected[" +
      "(leipzigF {city : \"Leipzig\", gender : \"f\", count : 1L})" +
      "(leipzigM {city : \"Leipzig\", gender : \"m\", count : 1L})" +
      "(dresdenF {city : \"Dresden\", gender : \"f\", count : 2L})" +
      "(dresdenM {city : \"Dresden\", gender : \"m\", count : 1L})" +
      "(berlinM  {city : \"Berlin\", gender : \"m\",  count : 1L})" +
      "(leipzigF)-[{count : 1L}]->(leipzigM)" +
      "(leipzigM)-[{count : 1L}]->(leipzigF)" +
      "(leipzigM)-[{count : 1L}]->(dresdenF)" +
      "(dresdenF)-[{count : 1L}]->(leipzigF)" +
      "(dresdenF)-[{count : 2L}]->(leipzigM)" +
      "(dresdenF)-[{count : 1L}]->(dresdenM)" +
      "(dresdenM)-[{count : 1L}]->(dresdenF)" +
      "(berlinM)-[{count : 1L}]->(dresdenF)" +
      "(berlinM)-[{count : 1L}]->(dresdenM)" +
      "]");

    TableLogicalGraph tInput = fromDataSetLogicalGraph(input);

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("city")
      .addVertexGroupingKey("gender")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(tInput.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);


    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexPropertyWithAbsentValue() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("g3");

    loader.appendToDatabaseFromString("expected[" +
      "(dresden {city : \"Dresden\", count : 2L})" +
      "(others  {city : " + NULL_STRING + ", count : 1L})" +
      "(others)-[{count : 3L}]->(dresden)" +
      "(dresden)-[{count : 1L}]->(dresden)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("city")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleVertexPropertiesWithAbsentValue() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("g3");

    loader.appendToDatabaseFromString("expected[" +
      "(dresdenF {city : \"Dresden\", gender : \"f\", count : 1L})" +
      "(dresdenM {city : \"Dresden\", gender : \"m\", count : 1L})" +
      "(others  {city : " + NULL_STRING + ", gender : " + NULL_STRING + ", count : 1L})" +
      "(others)-[{count : 2L}]->(dresdenM)" +
      "(others)-[{count : 1L}]->(dresdenF)" +
      "(dresdenF)-[{count : 1L}]->(dresdenM)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("city")
      .addVertexGroupingKey("gender")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexAndSingleEdgeProperty() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    TableLogicalGraph tInput = fromDataSetLogicalGraph(input);

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city : \"Leipzig\", count : 2L})" +
      "(dresden {city : \"Dresden\", count : 3L})" +
      "(berlin  {city : \"Berlin\",  count : 1L})" +
      "(dresden)-[{since : 2014, count : 2L}]->(dresden)" +
      "(dresden)-[{since : 2013, count : 2L}]->(leipzig)" +
      "(dresden)-[{since : 2015, count : 1L}]->(leipzig)" +
      "(leipzig)-[{since : 2014, count : 2L}]->(leipzig)" +
      "(leipzig)-[{since : 2013, count : 1L}]->(dresden)" +
      "(berlin)-[{since : 2015, count : 2L}]->(dresden)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("city")
      .addEdgeGroupingKey("since")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(tInput.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSingleVertexPropertyAndMultipleEdgeProperties() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("" +
      "input[" +
      "(v0 {a : 0,b : 0})" +
      "(v1 {a : 0,b : 1})" +
      "(v2 {a : 0,b : 1})" +
      "(v3 {a : 1,b : 0})" +
      "(v4 {a : 1,b : 1})" +
      "(v5 {a : 1,b : 0})" +
      "(v0)-[{a : 0,b : 1}]->(v1)" +
      "(v0)-[{a : 0,b : 2}]->(v2)" +
      "(v1)-[{a : 0,b : 3}]->(v2)" +
      "(v2)-[{a : 0,b : 2}]->(v3)" +
      "(v2)-[{a : 0,b : 1}]->(v3)" +
      "(v4)-[{a : 1,b : 2}]->(v2)" +
      "(v5)-[{a : 1,b : 3}]->(v2)" +
      "(v3)-[{a : 2,b : 3}]->(v4)" +
      "(v4)-[{a : 2,b : 1}]->(v5)" +
      "(v5)-[{a : 2,b : 0}]->(v3)" +
      "]"
    );

    loader.appendToDatabaseFromString("expected[" +
      "(v00 {a : 0,count : 3L})" +
      "(v01 {a : 1,count : 3L})" +
      "(v00)-[{a : 0,b : 1,count : 1L}]->(v00)" +
      "(v00)-[{a : 0,b : 2,count : 1L}]->(v00)" +
      "(v00)-[{a : 0,b : 3,count : 1L}]->(v00)" +
      "(v01)-[{a : 2,b : 0,count : 1L}]->(v01)" +
      "(v01)-[{a : 2,b : 1,count : 1L}]->(v01)" +
      "(v01)-[{a : 2,b : 3,count : 1L}]->(v01)" +
      "(v00)-[{a : 0,b : 1,count : 1L}]->(v01)" +
      "(v00)-[{a : 0,b : 2,count : 1L}]->(v01)" +
      "(v01)-[{a : 1,b : 2,count : 1L}]->(v00)" +
      "(v01)-[{a : 1,b : 3,count : 1L}]->(v00)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("a")
      .addEdgeGroupingKey("a")
      .addEdgeGroupingKey("b")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(loader.getTableLogicalGraphByVariable("input").getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleVertexAndMultipleEdgeProperties() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("" +
      "input[" +
      "(v0 {a : 0,b : 0})" +
      "(v1 {a : 0,b : 1})" +
      "(v2 {a : 0,b : 1})" +
      "(v3 {a : 1,b : 0})" +
      "(v4 {a : 1,b : 1})" +
      "(v5 {a : 1,b : 0})" +
      "(v0)-[{a : 0,b : 1}]->(v1)" +
      "(v0)-[{a : 0,b : 2}]->(v2)" +
      "(v1)-[{a : 0,b : 3}]->(v2)" +
      "(v2)-[{a : 0,b : 2}]->(v3)" +
      "(v2)-[{a : 0,b : 1}]->(v3)" +
      "(v4)-[{a : 1,b : 2}]->(v2)" +
      "(v5)-[{a : 1,b : 3}]->(v2)" +
      "(v3)-[{a : 2,b : 3}]->(v4)" +
      "(v4)-[{a : 2,b : 1}]->(v5)" +
      "(v5)-[{a : 2,b : 0}]->(v3)" +
      "]"
    );

    loader.appendToDatabaseFromString("expected[" +
      "(v00 {a : 0,b : 0,count : 1L})" +
      "(v01 {a : 0,b : 1,count : 2L})" +
      "(v10 {a : 1,b : 0,count : 2L})" +
      "(v11 {a : 1,b : 1,count : 1L})" +
      "(v00)-[{a : 0,b : 1,count : 1L}]->(v01)" +
      "(v00)-[{a : 0,b : 2,count : 1L}]->(v01)" +
      "(v01)-[{a : 0,b : 3,count : 1L}]->(v01)" +
      "(v01)-[{a : 0,b : 1,count : 1L}]->(v10)" +
      "(v01)-[{a : 0,b : 2,count : 1L}]->(v10)" +
      "(v11)-[{a : 2,b : 1,count : 1L}]->(v10)" +
      "(v10)-[{a : 2,b : 3,count : 1L}]->(v11)" +
      "(v10)-[{a : 2,b : 0,count : 1L}]->(v10)" +
      "(v10)-[{a : 1,b : 3,count : 1L}]->(v01)" +
      "(v11)-[{a : 1,b : 2,count : 1L}]->(v01)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("a")
      .addVertexGroupingKey("b")
      .addEdgeGroupingKey("a")
      .addEdgeGroupingKey("b")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(loader.getTableLogicalGraphByVariable("input").getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgePropertyWithAbsentValues() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("g3");

    loader.appendToDatabaseFromString("expected[" +
      "(dresden {city : \"Dresden\", count : 2L})" +
      "(others  {city : " + NULL_STRING + ", count : 1L})" +
      "(others)-[{since : 2013, count : 1L}]->(dresden)" +
      "(others)-[{since : " + NULL_STRING + ", count : 2L}]->(dresden)" +
      "(dresden)-[{since : 2014, count : 1L}]->(dresden)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .addVertexAggregateFunction(new Count("count"))
        .addEdgeAggregateFunction(new Count("count"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabel() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count : 6L})" +
      "(t:Tag     {count : 3L})" +
      "(f:Forum   {count : 2L})" +
      "(p)-[{count : 10L}]->(p)" +
      "(f)-[{count :  6L}]->(p)" +
      "(p)-[{count :  4L}]->(t)" +
      "(f)-[{count :  4L}]->(t)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .useVertexLabel(true)
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleVertexProperty() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    TableLogicalGraph tInput = fromDataSetLogicalGraph(input);

    loader.appendToDatabaseFromString("expected[" +
      "(l:Person {city : \"Leipzig\", count : 2L})" +
      "(d:Person {city : \"Dresden\", count : 3L})" +
      "(b:Person {city : \"Berlin\",  count : 1L})" +
      "(d)-[{count : 2L}]->(d)" +
      "(d)-[{count : 3L}]->(l)" +
      "(l)-[{count : 2L}]->(l)" +
      "(l)-[{count : 1L}]->(d)" +
      "(b)-[{count : 2L}]->(d)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("city")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(tInput.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleVertexPropertyWithAbsentValue() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city : \"Leipzig\", count : 2L})" +
      "(pD:Person {city : \"Dresden\", count : 3L})" +
      "(pB:Person {city : \"Berlin\",  count : 1L})" +
      "(t:Tag {city : " + NULL_STRING + ",   count : 3L})" +
      "(f:Forum {city : " + NULL_STRING + ", count : 2L})" +
      "(pD)-[{count : 2L}]->(pD)" +
      "(pD)-[{count : 3L}]->(pL)" +
      "(pL)-[{count : 2L}]->(pL)" +
      "(pL)-[{count : 1L}]->(pD)" +
      "(pB)-[{count : 2L}]->(pD)" +
      "(pB)-[{count : 1L}]->(t)" +
      "(pD)-[{count : 2L}]->(t)" +
      "(pL)-[{count : 1L}]->(t)" +
      "(f)-[{count : 3L}]->(pD)" +
      "(f)-[{count : 3L}]->(pL)" +
      "(f)-[{count : 4L}]->(t)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("city")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleEdgeProperty() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    TableLogicalGraph tInput = fromDataSetLogicalGraph(input);

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person {count : 6L})" +
      "(p)-[{since : 2014, count : 4L}]->(p)" +
      "(p)-[{since : 2013, count : 3L}]->(p)" +
      "(p)-[{since : 2015, count : 3L}]->(p)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .useVertexLabel(true)
      .addEdgeGroupingKey("since")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(tInput.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleEdgePropertyWithAbsentValue() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count : 6L})" +
      "(t:Tag     {count : 3L})" +
      "(f:Forum   {count : 2L})" +
      "(p)-[{since : 2014, count : 4L}]->(p)" +
      "(p)-[{since : 2013, count : 3L}]->(p)" +
      "(p)-[{since : 2015, count : 3L}]->(p)" +
      "(f)-[{since : 2013, count : 1L}]->(p)" +
      "(p)-[{since : " + NULL_STRING + ", count : 4L}]->(t)" +
      "(f)-[{since : " + NULL_STRING + ", count : 4L}]->(t)" +
      "(f)-[{since : " + NULL_STRING + ", count : 5L}]->(p)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .useVertexLabel(true)
      .addEdgeGroupingKey("since")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexLabelAndSingleVertexAndSingleEdgeProperty() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    TableLogicalGraph tInput = fromDataSetLogicalGraph(input);

    loader.appendToDatabaseFromString("expected[" +
      "(l:Person {city : \"Leipzig\", count : 2L})" +
      "(d:Person {city : \"Dresden\", count : 3L})" +
      "(b:Person {city : \"Berlin\",  count : 1L})" +
      "(d)-[{since : 2014, count : 2L}]->(d)" +
      "(d)-[{since : 2013, count : 2L}]->(l)" +
      "(d)-[{since : 2015, count : 1L}]->(l)" +
      "(l)-[{since : 2014, count : 2L}]->(l)" +
      "(l)-[{since : 2013, count : 1L}]->(d)" +
      "(b)-[{since : 2015, count : 2L}]->(d)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .useVertexLabel(true)
      .addVertexGroupingKey("city")
      .addEdgeGroupingKey("since")
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(tInput.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabel() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count : 6L})" +
      "(t:Tag     {count : 3L})" +
      "(f:Forum   {count : 2L})" +
      "(f)-[:hasModerator {count :  2L}]->(p)" +
      "(p)-[:hasInterest  {count :  4L}]->(t)" +
      "(f)-[:hasMember    {count :  4L}]->(p)" +
      "(f)-[:hasTag       {count :  4L}]->(t)" +
      "(p)-[:knows        {count : 10L}]->(p)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexProperty() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    TableLogicalGraph tInput = fromDataSetLogicalGraph(input);

    loader.appendToDatabaseFromString("expected[" +
      "(l:Person {city : \"Leipzig\", count : 2L})" +
      "(d:Person {city : \"Dresden\", count : 3L})" +
      "(b:Person {city : \"Berlin\",  count : 1L})" +
      "(d)-[:knows {count : 2L}]->(d)" +
      "(d)-[:knows {count : 3L}]->(l)" +
      "(l)-[:knows {count : 2L}]->(l)" +
      "(l)-[:knows {count : 1L}]->(d)" +
      "(b)-[:knows {count : 2L}]->(d)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("city")
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(tInput.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexPropertyWithAbsentValue() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city : \"Leipzig\", count : 2L})" +
      "(pD:Person {city : \"Dresden\", count : 3L})" +
      "(pB:Person {city : \"Berlin\", count : 1L})" +
      "(t:Tag   {city : " + NULL_STRING + ", count : 3L})" +
      "(f:Forum {city : " + NULL_STRING + ", count : 2L})" +
      "(pD)-[:knows {count : 2L}]->(pD)" +
      "(pD)-[:knows {count : 3L}]->(pL)" +
      "(pL)-[:knows {count : 2L}]->(pL)" +
      "(pL)-[:knows {count : 1L}]->(pD)" +
      "(pB)-[:knows {count : 2L}]->(pD)" +
      "(pB)-[:hasInterest {count : 1L}]->(t)" +
      "(pD)-[:hasInterest {count : 2L}]->(t)" +
      "(pL)-[:hasInterest {count : 1L}]->(t)" +
      "(f)-[:hasModerator {count : 1L}]->(pD)" +
      "(f)-[:hasModerator {count : 1L}]->(pL)" +
      "(f)-[:hasMember {count : 2L}]->(pD)" +
      "(f)-[:hasMember {count : 2L}]->(pL)" +
      "(f)-[:hasTag {count : 4L}]->(t)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("city")
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleEdgeProperty() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    TableLogicalGraph tInput = fromDataSetLogicalGraph(input);

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person {count : 6L})" +
      "(p)-[:knows {since : 2013, count : 3L}]->(p)" +
      "(p)-[:knows {since : 2014, count : 4L}]->(p)" +
      "(p)-[:knows {since : 2015, count : 3L}]->(p)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addEdgeGroupingKey("since")
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(tInput.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleEdgePropertyWithAbsentValue() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(p:Person  {count : 6L})" +
      "(t:Tag     {count : 3L})" +
      "(f:Forum   {count : 2L})" +
      "(p)-[:knows {since : 2014, count : 4L}]->(p)" +
      "(p)-[:knows {since : 2013, count : 3L}]->(p)" +
      "(p)-[:knows {since : 2015, count : 3L}]->(p)" +
      "(f)-[:hasModerator {since : 2013, count : 1L}]->(p)" +
      "(f)-[:hasModerator {since : " + NULL_STRING + ", count : 1L}]->(p)" +
      "(p)-[:hasInterest  {since : " + NULL_STRING + ", count : 4L}]->(t)" +
      "(f)-[:hasMember    {since : " + NULL_STRING + ", count : 4L}]->(p)" +
      "(f)-[:hasTag       {since : " + NULL_STRING + ", count : 4L}]->(t)" +

      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addEdgeGroupingKey("since")
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndVertexAndSingleEdgeProperty() throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph input = loader
      .getLogicalGraphByVariable("g0")
      .combine(loader.getLogicalGraphByVariable("g1"))
      .combine(loader.getLogicalGraphByVariable("g2"));

    TableLogicalGraph tInput = fromDataSetLogicalGraph(input);

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city : \"Leipzig\", count : 2L})" +
      "(pD:Person {city : \"Dresden\", count : 3L})" +
      "(pB:Person {city : \"Berlin\", count : 1L})" +
      "(pD)-[:knows {since : 2014, count : 2L}]->(pD)" +
      "(pD)-[:knows {since : 2013, count : 2L}]->(pL)" +
      "(pD)-[:knows {since : 2015, count : 1L}]->(pL)" +
      "(pL)-[:knows {since : 2014, count : 2L}]->(pL)" +
      "(pL)-[:knows {since : 2013, count : 1L}]->(pD)" +
      "(pB)-[:knows {since : 2015, count : 2L}]->(pD)" +
      "]");

    TableLogicalGraph tOutput = getGroupingBuilder()
      .addVertexGroupingKey("city")
      .addEdgeGroupingKey("since")
      .useVertexLabel(true)
      .useEdgeLabel(true)
      .addVertexAggregateFunction(new Count("count"))
      .addEdgeAggregateFunction(new Count("count"))
      .build()
      .execute(tInput.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexAndSingleEdgePropertyWithAbsentValue()
    throws Exception {
    TableFlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    TableLogicalGraph input = loader.getTableLogicalGraph();

    loader.appendToDatabaseFromString("expected[" +
      "(pL:Person {city : \"Leipzig\", count : 2L})" +
      "(pD:Person {city : \"Dresden\", count : 3L})" +
      "(pB:Person {city : \"Berlin\", count : 1L})" +
      "(t:Tag   {city : " + NULL_STRING + ", count : 3L})" +
      "(f:Forum {city : " + NULL_STRING + ", count : 2L})" +
      "(pD)-[:knows {since : 2014, count : 2L}]->(pD)" +
      "(pD)-[:knows {since : 2013, count : 2L}]->(pL)" +
      "(pD)-[:knows {since : 2015, count : 1L}]->(pL)" +
      "(pL)-[:knows {since : 2014, count : 2L}]->(pL)" +
      "(pL)-[:knows {since : 2013, count : 1L}]->(pD)" +
      "(pB)-[:knows {since : 2015, count : 2L}]->(pD)" +
      "(pB)-[:hasInterest {since : " + NULL_STRING + ", count : 1L}]->(t)" +
      "(pD)-[:hasInterest {since : " + NULL_STRING + ", count : 2L}]->(t)" +
      "(pL)-[:hasInterest {since : " + NULL_STRING + ", count : 1L}]->(t)" +
      "(f)-[:hasModerator {since : 2013, count : 1L}]->(pD)" +
      "(f)-[:hasModerator {since : " + NULL_STRING + ", count : 1L}]->(pL)" +
      "(f)-[:hasMember {since : " + NULL_STRING + ", count : 2L}]->(pD)" +
      "(f)-[:hasMember {since : " + NULL_STRING + ", count : 2L}]->(pL)" +
      "(f)-[:hasTag {since : " + NULL_STRING + ", count : 4L}]->(t)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .addVertexGroupingKey("city")
        .addEdgeGroupingKey("since")
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .addVertexAggregateFunction(new Count("count"))
        .addEdgeAggregateFunction(new Count("count"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  //----------------------------------------------------------------------------
  // Tests for aggregate functions
  //----------------------------------------------------------------------------

  @Test
  public void testNoAggregate() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v3:Red  {a : 4})" +
      "(v4:Red  {a : 2})" +
      "(v5:Red  {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 2}]->(v2)" +
      "(v2)-[{b : 3}]->(v3)" +
      "(v2)-[{b : 1}]->(v3)" +
      "(v3)-[{b : 3}]->(v4)" +
      "(v4)-[{b : 1}]->(v5)" +
      "(v5)-[{b : 1}]->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue)" +
      "(v01:Red)" +
      "(v00)-->(v00)" +
      "(v00)-->(v01)" +
      "(v01)-->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testCount() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v3:Red  {a : 4})" +
      "(v4:Red  {a : 2})" +
      "(v5:Red  {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 2}]->(v2)" +
      "(v2)-[{b : 3}]->(v3)" +
      "(v2)-[{b : 1}]->(v3)" +
      "(v3)-[{b : 3}]->(v4)" +
      "(v4)-[{b : 1}]->(v5)" +
      "(v5)-[{b : 1}]->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {count : 3L})" +
      "(v01:Red  {count : 3L})" +
      "(v00)-[{count : 3L}]->(v00)" +
      "(v00)-[{count : 2L}]->(v01)" +
      "(v01)-[{count : 3L}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new Count("count"))
        .addEdgeAggregateFunction(new Count("count"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSum() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v3:Red  {a : 4})" +
      "(v4:Red  {a : 2})" +
      "(v5:Red  {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 2}]->(v2)" +
      "(v2)-[{b : 3}]->(v3)" +
      "(v2)-[{b : 1}]->(v3)" +
      "(v3)-[{b : 3}]->(v4)" +
      "(v4)-[{b : 1}]->(v5)" +
      "(v5)-[{b : 1}]->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sumA :  9})" +
      "(v01:Red  {sumA : 10})" +
      "(v00)-[{sumB : 5}]->(v00)" +
      "(v00)-[{sumB : 4}]->(v01)" +
      "(v01)-[{sumB : 5}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new SumProperty("a", "sumA"))
        .addEdgeAggregateFunction(new SumProperty("b", "sumB"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSumWithMissingValue() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue)" +
      "(v2:Blue {a : 4})" +
      "(v3:Red  {a : 4})" +
      "(v4:Red  {a : 2})" +
      "(v5:Red  {a : 4})" +
      "(v0)-->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 2}]->(v2)" +
      "(v2)-[{b : 3}]->(v3)" +
      "(v2)-[{b : 1}]->(v3)" +
      "(v3)-[{b : 3}]->(v4)" +
      "(v4)-[{b : 1}]->(v5)" +
      "(v5)-[{b : 1}]->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sumA :  7})" +
      "(v01:Red  {sumA : 10})" +
      "(v00)-[{sumB : 3}]->(v00)" +
      "(v00)-[{sumB : 4}]->(v01)" +
      "(v01)-[{sumB : 5}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new SumProperty("a", "sumA"))
        .addEdgeAggregateFunction(new SumProperty("b", "sumB"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testSumWithMissingValues() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue)" +
      "(v1:Blue)" +
      "(v2:Blue)" +
      "(v3:Red)" +
      "(v4:Red)" +
      "(v5:Red)" +
      "(v0)-->(v1)" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "(v2)-->(v3)" +
      "(v3)-->(v4)" +
      "(v4)-->(v5)" +
      "(v5)-->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {sumA :  " + NULL_STRING + "})" +
      "(v01:Red  {sumA :  " + NULL_STRING + "})" +
      "(v00)-[{sumB : " + NULL_STRING + "}]->(v00)" +
      "(v00)-[{sumB : " + NULL_STRING + "}]->(v01)" +
      "(v01)-[{sumB : " + NULL_STRING + "}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new SumProperty("a", "sumA"))
        .addEdgeAggregateFunction(new SumProperty("b", "sumB"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMin() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v3:Red  {a : 4})" +
      "(v4:Red  {a : 2})" +
      "(v5:Red  {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 2}]->(v2)" +
      "(v2)-[{b : 3}]->(v3)" +
      "(v2)-[{b : 1}]->(v3)" +
      "(v3)-[{b : 3}]->(v4)" +
      "(v4)-[{b : 1}]->(v5)" +
      "(v5)-[{b : 1}]->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA : 2})" +
      "(v01:Red  {minA : 2})" +
      "(v00)-[{minB : 1}]->(v00)" +
      "(v00)-[{minB : 1}]->(v01)" +
      "(v01)-[{minB : 1}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MinProperty("a", "minA"))
        .addEdgeAggregateFunction(new MinProperty("b", "minB"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMinWithMissingValue() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue)" +
      "(v2:Blue {a : 4})" +
      "(v3:Red  {a : 4})" +
      "(v4:Red)" +
      "(v5:Red  {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-->(v2)" +
      "(v1)-[{b : 2}]->(v2)" +
      "(v2)-[{b : 3}]->(v3)" +
      "(v2)-->(v3)" +
      "(v3)-[{b : 3}]->(v4)" +
      "(v4)-->(v5)" +
      "(v5)-[{b : 1}]->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA : 3})" +
      "(v01:Red  {minA : 4})" +
      "(v00)-[{minB : 2}]->(v00)" +
      "(v00)-[{minB : 3}]->(v01)" +
      "(v01)-[{minB : 1}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MinProperty("a", "minA"))
        .addEdgeAggregateFunction(new MinProperty("b", "minB"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMinWithMissingValues() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue)" +
      "(v1:Blue)" +
      "(v2:Blue)" +
      "(v3:Red)" +
      "(v4:Red)" +
      "(v5:Red)" +
      "(v0)-->(v1)" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "(v2)-->(v3)" +
      "(v3)-->(v4)" +
      "(v4)-->(v5)" +
      "(v5)-->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA :  " + NULL_STRING + "})" +
      "(v01:Red  {minA :  " + NULL_STRING + "})" +
      "(v00)-[{minB : " + NULL_STRING + "}]->(v00)" +
      "(v00)-[{minB : " + NULL_STRING + "}]->(v01)" +
      "(v01)-[{minB : " + NULL_STRING + "}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MinProperty("a", "minA"))
        .addEdgeAggregateFunction(new MinProperty("b", "minB"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMax() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v3:Red  {a : 4})" +
      "(v4:Red  {a : 2})" +
      "(v5:Red  {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 2}]->(v2)" +
      "(v2)-[{b : 3}]->(v3)" +
      "(v2)-[{b : 1}]->(v3)" +
      "(v3)-[{b : 3}]->(v4)" +
      "(v4)-[{b : 1}]->(v5)" +
      "(v5)-[{b : 1}]->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {maxA : 4})" +
      "(v01:Red  {maxA : 4})" +
      "(v00)-[{maxB : 2}]->(v00)" +
      "(v00)-[{maxB : 3}]->(v01)" +
      "(v01)-[{maxB : 3}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MaxProperty("a", "maxA"))
        .addEdgeAggregateFunction(new MaxProperty("b", "maxB"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMaxWithMissingValue() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue)" +
      "(v3:Red)" +
      "(v4:Red  {a : 2})" +
      "(v5:Red  {a : 4})" +
      "(v0)-->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 2}]->(v2)" +
      "(v2)-->(v3)" +
      "(v2)-[{b : 1}]->(v3)" +
      "(v3)-->(v4)" +
      "(v4)-[{b : 1}]->(v5)" +
      "(v5)-[{b : 1}]->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {maxA : 3})" +
      "(v01:Red  {maxA : 4})" +
      "(v00)-[{maxB : 2}]->(v00)" +
      "(v00)-[{maxB : 1}]->(v01)" +
      "(v01)-[{maxB : 1}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MaxProperty("a", "maxA"))
        .addEdgeAggregateFunction(new MaxProperty("b", "maxB"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMaxWithMissingValues() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue)" +
      "(v1:Blue)" +
      "(v2:Blue)" +
      "(v3:Red)" +
      "(v4:Red)" +
      "(v5:Red)" +
      "(v0)-->(v1)" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "(v2)-->(v3)" +
      "(v3)-->(v4)" +
      "(v4)-->(v5)" +
      "(v5)-->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {maxA :  " + NULL_STRING + "})" +
      "(v01:Red  {maxA :  " + NULL_STRING + "})" +
      "(v00)-[{maxB : " + NULL_STRING + "}]->(v00)" +
      "(v00)-[{maxB : " + NULL_STRING + "}]->(v01)" +
      "(v01)-[{maxB : " + NULL_STRING + "}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MaxProperty("a", "maxA"))
        .addEdgeAggregateFunction(new MaxProperty("b", "maxB"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testMultipleAggregators() throws Exception {
    TableFlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {a : 2})" +
      "(v2:Blue {a : 4})" +
      "(v3:Red  {a : 4})" +
      "(v4:Red  {a : 2})" +
      "(v5:Red  {a : 4})" +
      "(v0)-[{b : 2}]->(v1)" +
      "(v0)-[{b : 1}]->(v2)" +
      "(v1)-[{b : 2}]->(v2)" +
      "(v2)-[{b : 3}]->(v3)" +
      "(v2)-[{b : 1}]->(v3)" +
      "(v3)-[{b : 3}]->(v4)" +
      "(v4)-[{b : 1}]->(v5)" +
      "(v5)-[{b : 1}]->(v3)" +
      "]");

    TableLogicalGraph input = loader.getTableLogicalGraphByVariable("input");

    loader.appendToDatabaseFromString("expected[" +
      "(v00:Blue {minA : 2,maxA : 4,sumA : 9,count : 3L})" +
      "(v01:Red  {minA : 2,maxA : 4,sumA : 10,count : 3L})" +
      "(v00)-[{minB : 1,maxB : 2,sumB : 5,count : 3L}]->(v00)" +
      "(v00)-[{minB : 1,maxB : 3,sumB : 4,count : 2L}]->(v01)" +
      "(v01)-[{minB : 1,maxB : 3,sumB : 5,count : 3L}]->(v01)" +
      "]");

    TableLogicalGraph tOutput =
      getGroupingBuilder()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MinProperty("a", "minA"))
        .addVertexAggregateFunction(new MaxProperty("a", "maxA"))
        .addVertexAggregateFunction(new SumProperty("a", "sumA"))
        .addVertexAggregateFunction(new Count("count"))
        .addEdgeAggregateFunction(new MinProperty("b", "minB"))
        .addEdgeAggregateFunction(new MaxProperty("b", "maxB"))
        .addEdgeAggregateFunction(new SumProperty("b", "sumB"))
        .addEdgeAggregateFunction(new Count("count"))
        .build()
        .execute(input.getLayout());

    LogicalGraph output = fromTableLogicalGraph(tOutput);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

}
