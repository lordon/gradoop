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
package org.gradoop.flink.io.impl.pubchem_asn.inputformats;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.GZIPInputStream;

import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;

import de.uni_leipzig.dbs.jasn1.pubchem.util.PubchemGzipInputStream;

/**
 * Factory for blocking input streams that decompress the GZIP compression format.
 */
public class BlockingGzipInflaterInputStreamFactory implements InflaterInputStreamFactory<GZIPInputStream> {

  /**
   * Field holding an instance
   */
  private static BlockingGzipInflaterInputStreamFactory INSTANCE = null;

  /**
   * Creates and/or returns instance.
   *
   * @return Instance of BlockingGzipInflaterInputStreamFactory
   */
  public static BlockingGzipInflaterInputStreamFactory getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new BlockingGzipInflaterInputStreamFactory();
    }
    return INSTANCE;
  }

  /**
   * Wraps given {@link InputStream} as {@link PubchemGzipInputStream} which is blocking.
   *
   * @param in is the compressed input stream
   * @return the inflated input stream
   * @throws IOException
   */
  @Override
  public GZIPInputStream create(final InputStream in) throws IOException {
    return new PubchemGzipInputStream(in);
  }

  /**
   * Lists a collection typical file extensions that are associated to {@link PubchemGzipInputStream}.
   *
   * @return collection of lower-case file extensions
   */
  @Override
  public Collection<String> getCommonFileExtensions() {
    return Arrays.asList("gz", "gzip");
  }

}
