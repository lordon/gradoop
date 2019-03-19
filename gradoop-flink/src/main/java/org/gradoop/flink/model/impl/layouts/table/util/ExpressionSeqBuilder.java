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
package org.gradoop.flink.model.impl.layouts.table.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

import static org.gradoop.flink.model.impl.layouts.table.util.ExpressionUtils.convertToSeq;

/**
 * Builder for a scala sequence of expressions, i.e. {@link Seq<Expression>} for use with Flink's
 * Table-API. Builder ist built upon {@link ExpressionBuilder}.
 * In contrast to the builder for single expressions, each completely built expression is added
 * to a list of expressions.
 *
 * Example usage equivalent to a SQL "foo AS a, bar AS b":
 * <pre>
 *  {@code
 *    ExpressionSeqBuilder builder = new ExpressionSeqBuilder();
 *    builder
 *      .field("foo")
 *      .as("a")
 *      .field("bar")
 *      .as("b")
 *      .buildSeq()
 *  }
 * </pre>
 */
public class ExpressionSeqBuilder extends ExpressionBuilder {

  /**
   * Internal list of expressions
   */
  private List<Expression> expressions;

  /**
   * Constructor
   */
  public ExpressionSeqBuilder() {
    this.expressions = new ArrayList<>();
  }

  /**
   * Returns scala sequence of expressions built with this builder
   * @return scala sequence of expressions
   */
  public Seq<Expression> buildSeq() {
    appendIfNewExpression();
    return convertToSeq(expressions);
  }

  /**
   * Returns true if there is no expression built yet
   *
   * @return true if there is no expression built yet
   */
  public boolean isEmpty() {
    appendIfNewExpression();
    return expressions.isEmpty();
  }

  //----------------------------------------------------------------------------
  // Operators which build completely new expressions. Former expression needs
  // to be added to the sequence.
  //----------------------------------------------------------------------------

  @Override
  public ExpressionSeqBuilder allFields() {
    appendIfNewExpression();
    super.allFields();
    return this;
  }

  @Override
  public ExpressionSeqBuilder expression(Expression e) {
    appendIfNewExpression();
    super.expression(e);
    return this;
  }

  @Override
  public ExpressionSeqBuilder field(String fieldName) {
    appendIfNewExpression();
    super.field(fieldName);
    return this;
  }

  @Override
  public ExpressionSeqBuilder resolvedField(String fieldName, TypeInformation<?> resultType) {
    appendIfNewExpression();
    super.resolvedField(fieldName, resultType);
    return this;
  }

  @Override
  public ExpressionSeqBuilder scalarFunctionCall(ScalarFunction function, Expression[] parameters) {
    appendIfNewExpression();
    super.scalarFunctionCall(function, parameters);
    return this;
  }

  @Override
  public ExpressionSeqBuilder scalarFunctionCall(ScalarFunction function, String... parameters) {
    appendIfNewExpression();
    super.scalarFunctionCall(function, parameters);
    return this;
  }

  @Override
  public ExpressionSeqBuilder aggFunctionCall(AggregateFunction function, Expression[] parameters) {
    appendIfNewExpression();
    super.aggFunctionCall(function, parameters);
    return this;
  }

  @Override
  public ExpressionSeqBuilder aggFunctionCall(AggregateFunction function, String... parameters) {
    appendIfNewExpression();
    super.aggFunctionCall(function, parameters);
    return this;
  }

  //----------------------------------------------------------------------------
  // Operators which build nested single expressions based on the former
  // expression. No need to add current expression to the sequence.
  //----------------------------------------------------------------------------

  @Override
  public ExpressionSeqBuilder as(String name, Seq<String> extraNames) {
    super.as(name, extraNames);
    return this;
  }

  @Override
  public ExpressionSeqBuilder as(String name) {
    super.as(name);
    return this;
  }

  @Override
  public ExpressionSeqBuilder and(Expression expression) {
    super.and(expression);
    return this;
  }

  @Override
  public ExpressionSeqBuilder equalTo(Expression expression) {
    super.equalTo(expression);
    return this;
  }

  @Override
  public ExpressionSeqBuilder equalTo(String fieldName) {
    super.equalTo(fieldName);
    return this;
  }

  /**
   * Appends the current expression of {@link ExpressionBuilder} to the sequence if it wasn't
   * added already before
   */
  private void appendIfNewExpression() {
    if (null != this.currentExpression && !expressions.contains(this.currentExpression)) {
      expressions.add(this.currentExpression);
    }
  }
}
