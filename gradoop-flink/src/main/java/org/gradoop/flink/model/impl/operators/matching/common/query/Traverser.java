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
package org.gradoop.flink.model.impl.operators.matching.common.query;

/**
 * Used to traverse a query graph.
 */
public interface Traverser {

  /**
   * Traverse the graph.
   *
   * @return traversal code
   */
  TraversalCode traverse();

  /**
   * Set the query handler to access the query graph.
   *
   * @param queryHandler query handler
   */
  void setQueryHandler(QueryHandler queryHandler);

  /**
   * Returns the query handler.
   *
   * @return query handler
   */
  QueryHandler getQueryHandler();

}
