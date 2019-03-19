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
import org.apache.flink.table.expressions.AggFunctionCall;
import org.apache.flink.table.expressions.Alias;
import org.apache.flink.table.expressions.And;
import org.apache.flink.table.expressions.EqualTo;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.expressions.ResolvedFieldReference;
import org.apache.flink.table.expressions.ScalarFunctionCall;
import org.apache.flink.table.expressions.UnresolvedFieldReference;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import scala.collection.Seq;

/**
 * Builder for a single (eventually nested) Flink Table {@link Expression} for use with Flink's
 * Table-API
 *
 * Example usage equivalent to a SQL "foo AS bar":
 * <pre>
 *  {@code
 *    ExpressionBuilder builder = new ExpressionBuilder();
 *    builder
 *      .field("foo")
 *      .as("bar")
 *      .toExpression()
 *  }
 * </pre>
 *
 * The builder does not perform any semantic check! Using correct operators is delegated to
 * the user.
 * Calling specific methods on a builder instance may lead to undo proceeding calls. Example:
 *
 * <pre>
 *  {@code
 *    ExpressionBuilder builder = new ExpressionBuilder();
 *    builder
 *      .field("foo")
 *      .as("bar")
 *      .field("foo2")
 *      .toExpression()
 *  }
 * </pre>
 *
 * The returned expression will only contain the field reference "foo2".
 *
 * Note it is possible to build senseless expressions like "foo AS bar IN (foo) AS bar".
 */
public class ExpressionBuilder {

  /**
   * Current expression object
   */
  protected Expression currentExpression;

  /**
   * Returns built expression object
   *
   * @return flink expression object
   */
  public Expression toExpression() {
    return currentExpression;
  }

  /**
   * Sets current expression to a "all fields" expression
   *
   * @return a reference to this object
   */
  public ExpressionBuilder allFields() {
    currentExpression = new UnresolvedFieldReference("*");
    return this;
  }

  /**
   * Sets given expression as current expression
   *
   * @param e expression
   * @return a reference to this object
   */
  public ExpressionBuilder expression(Expression e) {
    currentExpression = e;
    return this;
  }

  /**
   * Sets current expression to a field reference to field with given field name
   *
   * @param fieldName field name
   * @return a reference to this object
   */
  public ExpressionBuilder field(String fieldName) {
    currentExpression = new UnresolvedFieldReference(fieldName);
    return this;
  }

  /**
   * Sets current expression to a field reference to field with given field name with given type
   *
   * @param fieldName field name
   * @param resultType field type
   * @return a reference to this object
   */
  public ExpressionBuilder resolvedField(String fieldName, TypeInformation<?> resultType) {
    currentExpression = new ResolvedFieldReference(fieldName, resultType);
    return this;
  }

  /**
   * Sets current expression to a call of given scalar function with given parameters
   *
   * @param function table scalar function
   * @param parameters array of expressions as {@link Expression}
   * @return a reference to this object
   */
  public ExpressionBuilder scalarFunctionCall(ScalarFunction function, Expression[] parameters) {
    currentExpression = new ScalarFunctionCall(function, ExpressionUtils.convertToSeq(parameters));
    return this;
  }

  /**
   * Sets current expression to a call of given scalar function with given field names as parameters
   *
   * @param function table scalar function
   * @param parameters array of field names
   * @return a reference to this object
   */
  public ExpressionBuilder scalarFunctionCall(ScalarFunction function, String ... parameters) {
    return scalarFunctionCall(function,
      ExpressionUtils.convertStringArrayToFieldReferenceArray(parameters));
  }

  /**
   * Sets current expression to a call of given aggregation function with given parameters
   *
   * @param function table aggregation function
   * @param parameters array of expressions as {@link Expression}
   * @return a reference to this object
   */
  public ExpressionBuilder aggFunctionCall(AggregateFunction function, Expression[] parameters) {
    currentExpression = new AggFunctionCall(function, function.getResultType(),
      function.getAccumulatorType(), ExpressionUtils.convertToSeq(parameters));
    return this;
  }

  /**
   * Sets current expression to a call of given aggregation function with given field names as
   * parameters
   *
   * @param function table aggregation function
   * @param parameters array of field names
   * @return a reference to this object
   */
  public ExpressionBuilder aggFunctionCall(AggregateFunction function, String ... parameters) {
    return aggFunctionCall(function,
      ExpressionUtils.convertStringArrayToFieldReferenceArray(parameters));
  }

  /**
   * Appends an alias to current expression
   *
   * @param name alias name
   * @param extraNames extra names
   * @return a reference to this object
   */
  public ExpressionBuilder as(String name, Seq<String> extraNames) {
    currentExpression = new Alias(currentExpression, name, extraNames);
    return this;
  }

  /**
   * Appends an alias to current expression
   *
   * @param name alias name
   * @return a reference to this object
   */
  public ExpressionBuilder as(String name) {
    return as(name, ExpressionUtils. EMPTY_STRING_SEQ);
  }

  /**
   * Appends a call of SQL "IN(expressions...)" with given expressions to current expression
   * Builds a boolean expression!
   *
   * @param elements array of expressions
   * @return a reference to this object
   */
  public ExpressionBuilder in(Expression ... elements) {
    currentExpression = new In(currentExpression, ExpressionUtils.convertToSeq(elements));
    return this;
  }

  /**
   * Appends a call of SQL "IN('foo', 'bar', ..)" with given string literals to current expression
   * Builds a boolean expression!
   *
   * @param elements array of string literals
   * @return a reference to this object
   */
  public ExpressionBuilder in(String ... elements) {
    return in(ExpressionUtils.convertStringArrayToLiteralArray(elements));
  }


  /**
   * Appends a call of boolean "AND(expression)" operator with given expression to current
   * expression
   * Builds a boolean expression!
   *
   * @param expression expression
   * @return a reference to this object
   */
  public ExpressionBuilder and(Expression expression) {
    if (null == currentExpression) {
      currentExpression = expression;
    } else {
      currentExpression = new And(currentExpression, expression);
    }
    return this;
  }

  /**
   * Appends a call of boolean "=" operator with given expression to current expression
   * Builds a boolean expression!
   *
   * @param expression expression
   * @return a reference to this object
   */
  public ExpressionBuilder equalTo(Expression expression) {
    currentExpression = new EqualTo(currentExpression, expression);
    return this;
  }

  /**
   * Appends a call of boolean "=" operator with given field name to current expression
   * Builds a boolean expression!
   *
   * @param fieldName field name
   * @return a reference to this object
   */
  public ExpressionBuilder equalTo(String fieldName) {
    return equalTo(new UnresolvedFieldReference(fieldName));
  }

}
