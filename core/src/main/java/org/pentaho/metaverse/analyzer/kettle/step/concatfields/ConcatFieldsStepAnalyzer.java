/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.metaverse.analyzer.kettle.step.concatfields;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.steps.concatfields.ConcatFieldsMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileField;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.ChangeType;
import org.pentaho.metaverse.api.IComponentDescriptor;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.StepField;
import org.pentaho.metaverse.api.analyzer.kettle.ComponentDerivationRecord;
import org.pentaho.metaverse.api.analyzer.kettle.step.StepAnalyzer;
import org.pentaho.metaverse.api.model.Operation;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ConcatFieldsStepAnalyzer extends StepAnalyzer<ConcatFieldsMeta> {

  @Override
  protected Set<StepField> getUsedFields( ConcatFieldsMeta meta ) {
    HashSet<StepField> usedFields = new HashSet<>();
    for ( TextFileField outputField : meta.getOutputFields() ) {
      final Iterator<StepField> stepFields = createStepFields( outputField.getName(), getInputs() ).iterator();
      while ( stepFields.hasNext() ) {
        final StepField stepField = stepFields.next();
        usedFields.add( stepField );
      }
    }
    return usedFields;
  }

  @Override
  protected void addCustomFieldProperties( final IMetaverseNode stepFieldNode, final ValueMetaInterface fieldMeta ) {

    for ( TextFileField outputField :  baseStepMeta.getOutputFields() ) {
      if( outputField.getName().equalsIgnoreCase( fieldMeta.getName() ) ) {
        stepFieldNode.setProperty( "ifNull", outputField.getNullString() );
      }
    }
  }

  @Override
  public Set<Class<? extends BaseStepMeta>> getSupportedSteps() {
    Set<Class<? extends BaseStepMeta>> set = new HashSet<Class<? extends BaseStepMeta>>( 1 );
    set.add( ConcatFieldsMeta.class );
    return set;
  }

  @Override
  protected void customAnalyze( ConcatFieldsMeta meta, IMetaverseNode node )
    throws MetaverseAnalyzerException {
    node.setProperty( "targetFieldName", meta.getTargetFieldName() );
    node.setProperty( "targetFieldLength", meta.getTargetFieldLength() );
    node.setProperty( "separator", meta.getSeparator() );
    node.setProperty( DictionaryConst.PROPERTY_ENCLOSURE, meta.getEnclosure() );
  }

  @Override
  protected IMetaverseNode createNodeFromDescriptor( IComponentDescriptor descriptor ) {
    final IMetaverseNode stepNode = super.createNodeFromDescriptor( descriptor );
    return stepNode;
  }

  @Override
  public Set<ComponentDerivationRecord> getChangeRecords( ConcatFieldsMeta meta )
    throws MetaverseAnalyzerException {

    final Set<ComponentDerivationRecord> changeRecords = new HashSet<ComponentDerivationRecord>();
    final HashSet<String> usedFieldNames = new HashSet<>();

    final String targetFieldName = meta.getTargetFieldName();

    for ( final TextFileField outputField : meta.getOutputFields() ) {
      usedFieldNames.add( outputField.getName() );
    }

    for ( String usedFieldName : usedFieldNames ) {
      final ComponentDerivationRecord changeRecord =
        new ComponentDerivationRecord( usedFieldName, targetFieldName, ChangeType.METADATA );
      changeRecord.addOperation( new Operation( Operation.AGG_CATEGORY, ChangeType.METADATA, DictionaryConst
        .PROPERTY_TRANSFORMS, "[ " + meta.getEnclosure() + "<" + StringUtils.join( usedFieldNames, ">" + meta
        .getSeparator() + "<" ) + ">" + meta.getEnclosure() + " ] -> " + targetFieldName ) );
      changeRecords.add( changeRecord );
    }
    return changeRecords;
  }
}
