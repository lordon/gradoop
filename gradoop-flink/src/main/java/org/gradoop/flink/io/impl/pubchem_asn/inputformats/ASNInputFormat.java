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

import de.uni_leipzig.dbs.jasn1.pubchem.compounds.pcsubstance.PCCompound;
import de.uni_leipzig.dbs.jasn1.pubchem.util.PCAtomsFilter;
import de.uni_leipzig.dbs.jasn1.pubchem.util.PCCompoundFilter;
import de.uni_leipzig.dbs.jasn1.pubchem.util.PropsFilter;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.openmuc.jasn1.ber.BerLength;
import org.openmuc.jasn1.ber.BerTag;

import java.io.EOFException;
import java.io.IOException;

/**
 * Provides a {@link FileInputFormat} for Pubchem binary ASN files.
 * Pubchem's ASN files are unsplittable because of their indefinite-length based encoding.
 * @see <a href="https://pubchem.ncbi.nlm.nih.gov/">https://pubchem.ncbi.nlm.nih.gov</a>
 */
public class ASNInputFormat extends FileInputFormat {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2823148350559555310L;

  /**
   * All-embracing ASN tag of a PC-Compounds file
   */
  private static final BerTag PC_COMPOUNDS_TAG =
          new BerTag(BerTag.UNIVERSAL_CLASS, BerTag.CONSTRUCTED, 16);

  /**
   * true, if end of current file was reached
   */
  private boolean reachedEnd;

  /**
   * true, if properties shall be stored in POJO's
   */
  private boolean withProperties;

  /**
   * Filter instance passed to parser indicating which properties shall be stored in POJO's
   */
  private transient PCCompoundFilter filter;

  /**
   * Creates unsplittable {@link FileInputFormat} instance for given file
   *
   * @param filePath        Path to pubchem ASN file
   * @param withProperties  true, if properties shall be stored in POJO's
   */
  public ASNInputFormat(final String filePath, boolean withProperties) {
    this.setFilePath(filePath);
    this.withProperties = withProperties;
    this.unsplittable = true;
  }

  /**
   * Checks whether the current file is at its end.
   *
   * @return True, if the split is at its end, false otherwise.
   * @throws IOException
   */
  @Override
  public boolean reachedEnd() throws IOException {
    return this.reachedEnd;
  }

  /**
   * Indicates that each file is unsplittable.
   *
   * @param pathFile  Path to pubchem ASN file
   * @return True
   */
  @Override
  protected boolean testForUnsplittable(final FileStatus pathFile) {
    return true;
  }


  /**
   * Returns next {@link PCCompound} record in file.
   *
   * @param o unused
   * @return PCCompound instance
   * @throws IOException
   */
  @Override
  public PCCompound nextRecord(final Object o) throws IOException {
    BerTag berTag = new BerTag();
    berTag.decode(this.stream);

    // Handle end of file
    if (berTag.tagNumber == 0 && berTag.tagClass == 0 && berTag.primitive == 0) {
      int nextByte = this.stream.read();
      if (nextByte != 0) {
        if (nextByte == -1) {
          throw new EOFException("Unexpected end of input stream.");
        }
        throw new IOException("Decoded sequence has wrong end of contents octets");
      }
      reachedEnd = true;
      return null;
    }

    PCCompound element = new PCCompound(filter);
    element.decode(this.stream, false);
    return element;
  }

  /**
   * Opens an input stream to the file defined in the input format.
   * Each {@link FileInputSplit} is a whole binary ASN file.
   * Decodes the all-embracing ASN tag of file.
   *
   * @param split  Current {@link FileInputSplit} of pubchem ASN file
   * @throws IOException
   */
  @Override
  public void open(final FileInputSplit split) throws IOException {
    registerInflaterInputStreamFactory("gz", BlockingGzipInflaterInputStreamFactory.getInstance());

    this.reachedEnd = false;
    super.open(split);

    initializeFilter();

    // Start ASN decoding of all-embracing ASN tag and length tag
    PC_COMPOUNDS_TAG.decodeAndCheck(this.stream);
    BerLength length = new BerLength();
    length.decode(this.stream);

    if (length.val > -1) {
      throw new IOException("Expected ASN encoding with indefinite length.");
    }
  }

  /**
   * Initializes the {@link PCCompoundFilter} fulfilling the needs (considering weather properties
   * shall get parsed or not)
   */
  private void initializeFilter() {
    this.filter = new PCCompoundFilter();

    // Always skip some parts
    filter.setParseCoords(false);
    filter.setParseStereo(false);
    filter.setParseStereogroups(false);
    filter.setParseVbalt(false);

    if (withProperties) {
      // Store atom properties in POJO's
      filter.setAtomsFilter(new PCAtomsFilter(true, false));
    } else {
      // Do not store compound properties in POJO's
      PropsFilter propsFilter = new PropsFilter();
      propsFilter.setSkipMetadata(true);
      filter.setPropsFilter(propsFilter);
    }
  }
}
