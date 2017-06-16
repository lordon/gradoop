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
package org.gradoop.examples.io;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.pubchem_asn.PubchemASNDataSource;
import org.gradoop.flink.io.impl.tlf.TLFDataSink;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Example program that reads {@link GraphTransactions} from pubchem binary ASN files and stores
 * the result as TLF.
 * @see <a href="https://pubchem.ncbi.nlm.nih.gov/">https://pubchem.ncbi.nlm.nih.gov</a>
 */
public class PubchemASNExample extends AbstractRunner implements ProgramDescription {

  /**
   * Reads {@link GraphTransactions} from a directory that contains the separate
   * pubchem ASN files. Files can be stored in local file system or HDFS.
   * Writes graphs in TLF format to specified output location.
   *
   * args[0]: path to read ASN files from
   * args[1]: path to write DOT files to
   *
   * @param args program arguments
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException("provide input directory and output directory");
    }
    final String inputDir = args[0];
    final String targetDir = args[1];
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    PubchemASNDataSource dataSource = new PubchemASNDataSource(inputDir, config);
    GraphTransactions compounds = dataSource.getGraphTransactions();

    TLFDataSink dataSink = new TLFDataSink(targetDir, config);
    dataSink.write(compounds);

    env.execute();
  }

  @Override
  public String getDescription() {
    return PubchemASNExample.class.getName();
  }
}
