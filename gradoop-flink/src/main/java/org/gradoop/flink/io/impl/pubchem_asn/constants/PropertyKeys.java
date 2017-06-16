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
package org.gradoop.flink.io.impl.pubchem_asn.constants;

/**
 * Constants for known pubchem property keys
 */
public class PropertyKeys {

  /**
   * Property key for atom property label
   */
  public static final String PUBCHEM_ATOM_PROPERTY_LABEL = "label";

  /**
   * Property key for atom property isotope
   */
  public static final String PUBCHEM_ATOM_PROPERTY_ISOTOPE = "isotope";

  /**
   * Property key for atom property charge
   */
  public static final String PUBCHEM_ATOM_PROPERTY_CHARGE = "charge";

  /**
   * Property key for atom property comment
   */
  public static final String PUBCHEM_ATOM_PROPERTY_COMMENT = "comment";

  /**
   * Property key for graph property compoundType
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_TYPE = "compoundType";

  /**
   * Property key for graph property cid
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_CID = "cid";

  /**
   * Property key for graph property sid
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_SID = "sid";

  /**
   * Property key for graph property xid
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_XID = "xid";

  /**
   * Property key for graph property charge
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_CHARGE = "charge";

  /**
   * Property key for graph property heavyAtomCount
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_COUNT_HEAVY_ATOM = "heavyAtom";

  /**
   * Property key for graph property atomChiralCount
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_COUNT_ATOM_CHIRAL = "atomChiral";

  /**
   * Property key for graph property atomChiralDefCount
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_COUNT_ATOM_CHIRAL_DEF = "atomChiralDef";

  /**
   * Property key for graph property atomChiralUndefCount
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_COUNT_ATOM_CHIRAL_UNDEF = "atomChiralUndef";

  /**
   * Property key for graph property bondChiralCount
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_COUNT_BOND_CHIRAL = "bondChiral";

  /**
   * Property key for graph property condChiralDefCount
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_COUNT_BOND_CHIRAL_DEF = "bondChiralDef";

  /**
   * Property key for graph property condChiralUndefCount
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_COUNT_BOND_CHIRAL_UNDEF = "bondChiralUndef";

  /**
   * Property key for graph property isotopeAtomCount
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_COUNT_ISOTOPE_ATOM = "isotopeAtom";

  /**
   * Property key for graph property covalentUnitCount
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_COUNT_COVALENT_UNIT = "covalentUnit";

  /**
   * Property key for graph property tautomersCount
   */
  public static final String PUBCHEM_GRAPH_PROPERTY_COUNT_TAUTOMERS = "tautomers";

}
