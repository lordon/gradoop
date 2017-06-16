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
package org.gradoop.flink.io.impl.pubchem_asn.functions;

import com.google.common.collect.Sets;
import de.uni_leipzig.dbs.jasn1.pubchem.compounds.pcsubstance.PCAtomInt;
import de.uni_leipzig.dbs.jasn1.pubchem.compounds.pcsubstance.PCAtomString;
import de.uni_leipzig.dbs.jasn1.pubchem.compounds.pcsubstance.PCAtoms;
import de.uni_leipzig.dbs.jasn1.pubchem.compounds.pcsubstance.PCCompound;
import de.uni_leipzig.dbs.jasn1.pubchem.compounds.pcsubstance.PCCompoundType;
import de.uni_leipzig.dbs.jasn1.pubchem.compounds.pcsubstance.PCCount;
import de.uni_leipzig.dbs.jasn1.pubchem.compounds.pcsubstance.PCInfoData;
import de.uni_leipzig.dbs.jasn1.pubchem.compounds.pcsubstance.type.custom.BerRealString;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.pubchem_asn.constants.PropertyKeys;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.openmuc.jasn1.ber.types.BerBoolean;
import org.openmuc.jasn1.ber.types.BerInteger;
import org.openmuc.jasn1.ber.types.string.BerVisibleString;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Maps an instance of {@link PCCompound} to a {@link GraphTransaction}.
 * Atoms of the compound become vertices, bonds become edges.
 * Optionally, some compound/atom attributes become graph/vertex properties.
 */
public class CompoundToGraphTransaction implements MapFunction<PCCompound, GraphTransaction> {

  /**
   * Used to instantiate graph heads.
   */
  private final GraphHeadFactory graphHeadFactory;

  /**
   * Used to instantiate vertices.
   */
  private final VertexFactory vertexFactory;

  /**
   * Used to instantiate edges.
   */
  private final EdgeFactory edgeFactory;

  /**
   * True, if graph shall contain compound/atom properties.
   */
  private final boolean withProperties;

  /**
   * @param graphHeadFactory  graph head factory
   * @param vertexFactory     vertex factory
   * @param edgeFactory       edge factory
   * @param withProperties    true, if graph shall contain compound/atom properties
   */
  public CompoundToGraphTransaction(GraphHeadFactory graphHeadFactory, VertexFactory
          vertexFactory, EdgeFactory edgeFactory, boolean withProperties) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
    this.withProperties = withProperties;
  }

  @Override
  public GraphTransaction map(PCCompound compound) throws Exception {
    GraphHead graphHead = this.graphHeadFactory.createGraphHead();

    BerInteger cid = compound.getId().getId().getCid();
    if (null != cid) {
      graphHead.setLabel(cid.toString());
    }

    Set<Edge> edges = Sets.newHashSet();
    GradoopIdList graphIds = GradoopIdList.fromExisting(graphHead.getId());

    int atomCount = compound.getAtoms().getAid().getBerInteger().size();
    Vertex[] atomIdMap = new Vertex[atomCount];

    // Atoms become vertices, atom elements become atom labels
    for (int i = 0; i < atomCount; i++) {
      String element = compound.getAtoms().getElement().getPCElement().get(i).toString();
      Vertex atom = vertexFactory.createVertex(element, graphIds);
      atomIdMap[i] = atom;
    }

    // Bonds become edges, bond types become edge labels
    if (null != compound.getBonds()) {
      for (int i = 0; i < compound.getBonds().getAid1().getBerInteger().size(); i++) {
        int a1 = compound.getBonds().getAid1().getBerInteger().get(i).intValue() - 1;
        int a2 = compound.getBonds().getAid2().getBerInteger().get(i).intValue() - 1;
        String bondType = compound.getBonds().getOrder().getPCBondType().get(i).toString();
        bondType = bondType.substring(0, bondType.indexOf(" "));
        Edge bond = edgeFactory.createEdge(bondType, atomIdMap[a1].getId(), atomIdMap[a2].getId()
                , graphIds);
        edges.add(bond);
      }
    }

    if (this.withProperties) {
      // Compound/atom attributes become graph/vertex properties
      setAtomProperties(compound, atomIdMap);
      setCompoundProperties(compound, graphHead);
    }

    return new GraphTransaction(graphHead, Sets.newHashSet(atomIdMap), edges);
  }

  /**
   * Initiates creation of atom properties like lable, isotope, charge, comment
   *
   * @param compound    PCCompound instance which holds necessary data
   * @param atomIdMap   Array of atom's GradoopIds
   */
  private void setAtomProperties(PCCompound compound, Vertex[] atomIdMap) {
    PCAtoms.Label label = compound.getAtoms().getLabel();
    if (null != label) {
      processAtomString(atomIdMap, label.getPCAtomString(),
              PropertyKeys.PUBCHEM_ATOM_PROPERTY_LABEL);
    }

    PCAtoms.Isotope isotope = compound.getAtoms().getIsotope();
    if (null != isotope) {
      processAtomInt(atomIdMap, isotope.getPCAtomInt(),
              PropertyKeys.PUBCHEM_ATOM_PROPERTY_ISOTOPE);
    }

    PCAtoms.Charge charge = compound.getAtoms().getCharge();
    if (null != charge) {
      processAtomInt(atomIdMap, charge.getPCAtomInt(),
              PropertyKeys.PUBCHEM_ATOM_PROPERTY_CHARGE);
    }

    PCAtoms.Comment comment = compound.getAtoms().getComment();
    if (null != comment) {
      processAtomString(atomIdMap, comment.getPCAtomString(),
              PropertyKeys.PUBCHEM_ATOM_PROPERTY_COMMENT);
    }
  }

  /**
   * Performs creation of atom string properties
   *
   * @param atomIdMap   Array of atom's GradoopIds
   * @param container   Pubchem array of atom properties, e.g. PCAtoms.getPCAtomString()
   * @param propertyKey Key for new property
   */
  private void processAtomString(Vertex[] atomIdMap, List<PCAtomString> container, String
          propertyKey) {
    for (PCAtomString pair : container) {
      int aId = pair.getAid().intValue() - 1;
      Vertex atom = atomIdMap[aId];
      List<PropertyValue> labelList = atom.hasProperty(propertyKey) ? atom.getPropertyValue
              (propertyKey).getList() : new ArrayList<PropertyValue>();
      labelList.add(PropertyValue.create(pair.getValue().toString()));
      atom.setProperty(propertyKey, labelList);
    }
  }

  /**
   * Performs creation of atom int properties
   *
   * @param atomIdMap   Array of atom's GradoopIds
   * @param container   Pubchem array of atom properties, e.g. PCAtoms.getPCAtomInt()
   * @param propertyKey Key for new property
   */
  private void processAtomInt(Vertex[] atomIdMap, List<PCAtomInt> container, String propertyKey) {
    for (PCAtomInt pair : container) {
      int aId = pair.getAid().intValue() - 1;
      Vertex atom = atomIdMap[aId];
      List<PropertyValue> labelList = atom.hasProperty(propertyKey) ? atom.getPropertyValue
              (propertyKey).getList() : new ArrayList<PropertyValue>();
      labelList.add(PropertyValue.create(pair.getValue().intValue()));
      atom.setProperty(propertyKey, labelList);
    }
  }

  /**
   * Performs creation of severel compound properties like ids, props, counts
   *
   * @param compound    PCCompound instance which holds necessary data
   * @param graphHead   GraphHead which will have new properties
   */
  private void setCompoundProperties(PCCompound compound, GraphHead graphHead) {
    // ids
    PCCompoundType.PCCompoundTypeType compoundType = compound.getId().getType();
    if (null != compoundType) {
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_TYPE, compoundType.toString());
    }

    BerInteger cid = compound.getId().getId().getCid();
    if (null != cid) {
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_CID, cid.toString());
    }

    BerInteger sid = compound.getId().getId().getSid();
    if (null != sid) {
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_SID, sid.toString());
    }

    BerInteger xid = compound.getId().getId().getXid();
    if (null != xid) {
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_XID, xid.toString());
    }

    BerInteger charge = compound.getCharge();
    if (null != charge) {
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_CHARGE, charge.intValue());
    }

    // props
    PCCompound.Props props = compound.getProps();
    if (null != compound.getProps()) {
      for (PCInfoData prop : props.getPCInfoData()) {
        String propertyKey = prop.getUrn().getLabel() + "" + prop.getUrn().getName();
        propertyKey = propertyKey.replaceAll("\\s", "");
        propertyKey = propertyKey.replaceAll("-", "");
        Object value = extractPropValue(prop.getValue());
        if (null != value) {
          graphHead.setProperty(propertyKey, value);
        }
      }
    }

    // counts
    PCCount count = compound.getCount();
    if (null != count) {
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_COUNT_HEAVY_ATOM,
              count.getHeavyAtom().intValue());
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_COUNT_ATOM_CHIRAL,
              count.getAtomChiral().intValue());
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_COUNT_ATOM_CHIRAL_DEF,
              count.getAtomChiralDef().intValue());
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_COUNT_ATOM_CHIRAL_UNDEF,
              count.getAtomChiralUndef().intValue());
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_COUNT_BOND_CHIRAL,
              count.getBondChiral().intValue());
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_COUNT_BOND_CHIRAL_DEF,
              count.getBondChiralDef().intValue());
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_COUNT_BOND_CHIRAL_UNDEF,
              count.getBondChiralUndef().intValue());
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_COUNT_ISOTOPE_ATOM,
              count.getIsotopeAtom().intValue());
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_COUNT_COVALENT_UNIT,
              count.getCovalentUnit().intValue());
      graphHead.setProperty(PropertyKeys.PUBCHEM_GRAPH_PROPERTY_COUNT_TAUTOMERS,
              count.getTautomers().intValue());
    }
  }

  /**
   * Extracts value from pubchem's properties. Each possible type has to be checked because of ASN's
   * static type system. One can't know which data type some property has.
   *
   * @param value Instance of PCInfoData.Value
   * @return Value of pubchem property in matching type, e.g. boolean, List<Boolean>, ..
   */
  private Object extractPropValue(PCInfoData.Value value) {
    BerBoolean bval = value.getBval();
    if (null != bval) {
      return bval.value;
    }

    PCInfoData.Value.Bvec bvec = value.getBvec();
    if (null != bvec) {
      List<Boolean> vals = new ArrayList<Boolean>();
      for (BerBoolean val : bvec.getBerBoolean()) {
        vals.add(val.value);
      }
      return vals;
    }

    BerInteger ival = value.getIval();
    if (null != ival) {
      return ival.intValue();
    }

    PCInfoData.Value.Ivec ivec = value.getIvec();
    if (null != ivec) {
      List<Integer> vals = new ArrayList<Integer>();
      for (BerInteger val : ivec.getBerInteger()) {
        vals.add(val.intValue());
      }
      return vals;
    }

    BerRealString fval = value.getFval();
    if (null != fval) {
      return fval.getValue();
    }

    PCInfoData.Value.Fvec fvec = value.getFvec();
    if (null != fvec) {
      List<Double> vals = new ArrayList<Double>();
      for (BerRealString val : fvec.getBerReal()) {
        vals.add(val.getValue());
      }
      return vals;
    }

    BerVisibleString sval = value.getSval();
    if (null != sval) {
      return sval.toString();
    }

    PCInfoData.Value.Slist slist = value.getSlist();
    if (null != slist) {
      List<String> vals = new ArrayList<String>();
      for (BerVisibleString val : slist.getBerVisibleString()) {
        vals.add(val.toString());
      }
      return vals;
    }
    return null;
  }
}
