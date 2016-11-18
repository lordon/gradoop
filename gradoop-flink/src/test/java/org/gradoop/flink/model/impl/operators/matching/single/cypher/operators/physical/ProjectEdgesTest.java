/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;

import org.junit.Test;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class ProjectEdgesTest extends PhysicalOperatorTest {

  @Test
  public void returnsEmbeddingWithIdProjectionId() throws Exception{
    DataSet<Edge> edgeDataSet = createEdgesWithProperties(Lists.newArrayList("foo", "bar", "baz"));

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList("foo", "bar");
    ProjectEdges operator = new ProjectEdges(edgeDataSet, extractedPropertyKeys);

    DataSet<Embedding> results = operator.evaluate();

    assertEquals(2,results.count());
    assertEveryEmbedding(results, (embedding) -> {
      assertEquals(3,embedding.size());

      assertEquals(IdEntry.class,         embedding.getEntry(0).getClass());
      assertEquals(ProjectionEntry.class, embedding.getEntry(1).getClass());
      assertEquals(IdEntry.class,         embedding.getEntry(2).getClass());

      assertEquals(
        Sets.newHashSet(extractedPropertyKeys),
        embedding.getEntry(1).getProperties().get().getKeys()
      );
    });
  }
}