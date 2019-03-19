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

import org.apache.flink.table.api.Types;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.UnresolvedFieldReference;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Utils to work with Flink's table expressions and scala sequences of expressions.
 */
public class ExpressionUtils {

  /**
   * Empty scala sequence of strings
   */
  public static final Seq<String> EMPTY_STRING_SEQ = JavaConverters
    .asScalaIteratorConverter((new ArrayList<String>()).iterator()).asScala().toSeq();

  /**
   * Converts given array of expressions to a scala sequence of expressions
   *
   * @param array array of expressions
   * @return scala sequence of expressions
   */
  public static Seq<Expression> convertToSeq(Expression[] array) {
    return convertToSeq(new ArrayList<Expression>(Arrays.asList(array)));
  }

  /**
   * Converts given collection of expressions to a scala sequence of expressions
   *
   * @param collection collection of expressions
   * @return scala sequence of expressions
   */
  public static Seq<Expression> convertToSeq(Collection<Expression> collection) {
    return JavaConverters.asScalaIteratorConverter(collection.iterator()).asScala().toSeq();
  }

  /**
   * Converts given array of field name strings to a array of field reference expressions
   *
   * @param array array of field name strings
   * @return array of field reference expressions
   */
  public static Expression[] convertStringArrayToFieldReferenceArray(String ... array) {
    return Arrays.stream(array)
      .map(fieldName -> new UnresolvedFieldReference(fieldName)).toArray(Expression[]::new);
  }

  /**
   * Converts given array of string literals to a array of literal expressions
   *
   * @param array array of string literals
   * @return array of literal expressions
   */
  public static Expression[] convertStringArrayToLiteralArray(String ... array) {
    return Arrays.stream(array)
      .map(str -> new Literal(str, Types.STRING())).toArray(Expression[]::new);
  }
}
