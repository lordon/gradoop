/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.pubchem_asn;

import de.uni_leipzig.dbs.jasn1.pubchem.compounds.pcsubstance.PCCompound;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.pubchem_asn.functions.CompoundToGraphTransaction;
import org.gradoop.flink.io.impl.pubchem_asn.inputformats.ASNInputFormat;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Creates a {@link GraphCollection} from one or many Pubchem binary ASN files.
 * A logical EPGM graph represents one chemical compound.
 * @see <a href="https://pubchem.ncbi.nlm.nih.gov/">https://pubchem.ncbi.nlm.nih.gov</a>
 */
public class PubchemASNDataSource implements DataSource {

  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * File/Dir to read ASN files
   */
  private String inputPath;

  /**
   * True, if graphs shall contain compound properties
   */
  private boolean withProperties;

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param inputPath   directory containing Pubchem ASN files
   * @param config      Gradoop Flink configuration
   */
  public PubchemASNDataSource(String inputPath, GradoopFlinkConfig config) {
    this(inputPath, config, false);
  }

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param inputPath       directory containing Pubchem ASN files
   * @param config          Gradoop Flink configuration
   * @param withProperties  true, if graphs shall contain compound properties
   */
  public PubchemASNDataSource(String inputPath, GradoopFlinkConfig config, boolean withProperties) {
    this.inputPath = inputPath;
    this.config = config;
    this.withProperties = withProperties;
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException/**/ {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return GraphCollection.fromTransactions(getGraphTransactions());
  }

  @Override
  public GraphTransactions getGraphTransactions() {
    ExecutionEnvironment env = this.config.getExecutionEnvironment();

    // Load PCCompounds
    DataSet<PCCompound> dataSet = env.createInput(
      new ASNInputFormat(this.inputPath, this.withProperties),
      TypeInformation.of(PCCompound.class)
    );

    // Map each PCCompound to a single GraphTransaction
    DataSet<GraphTransaction> transactions =
      dataSet.map(new CompoundToGraphTransaction(
        config.getGraphHeadFactory(),
        config.getVertexFactory(),
        config.getEdgeFactory(),
        this.withProperties
      ));

    return new GraphTransactions(transactions, this.config);
  }
}
