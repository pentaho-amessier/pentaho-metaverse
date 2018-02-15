/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.metaverse.analyzer.kettle.step.textfileoutput;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileField;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.IMetaverseObjectFactory;
import org.pentaho.metaverse.api.MetaverseException;
import org.pentaho.metaverse.api.StepField;
import org.pentaho.metaverse.api.analyzer.kettle.step.ExternalResourceStepAnalyzer;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The TextFileOutputStepAnalyzer is responsible for providing nodes and links (i.e. relationships) between for the
 * fields operated on by Text File Output steps.
 */
public class TextFileOutputStepAnalyzer extends ExternalResourceStepAnalyzer<TextFileOutputMeta> {

  private Logger log = LoggerFactory.getLogger( TextFileOutputStepAnalyzer.class );

  @Override
  protected Set<StepField> getUsedFields( TextFileOutputMeta meta ) {
    Set<StepField> usedFields = new HashSet<>();
    if ( meta.isFileNameInField() ) {
      usedFields.addAll( createStepFields( meta.getFileNameField(), getInputs() ) );
    }
    return usedFields;
  }

  @Override
  public Set<Class<? extends BaseStepMeta>> getSupportedSteps() {
    return new HashSet<Class<? extends BaseStepMeta>>() {
      {
        add( TextFileOutputMeta.class );
      }
    };
  }

  @Override
  public String getResourceInputNodeType() {
    return null;
  }

  @Override
  public String getResourceOutputNodeType() {
    return DictionaryConst.NODE_TYPE_FILE_FIELD;
  }

  @Override
  public boolean isOutput() {
    return true;
  }

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public IMetaverseNode createResourceNode( IExternalResourceInfo resource ) throws MetaverseException {
    return createFileNode( resource.getName(), descriptor );
  }

  @Override
  public Set<String> getOutputResourceFields( TextFileOutputMeta meta ) {
    final Set<String> fields = new HashSet<>();
    final TextFileField[] outputFields = meta.getOutputFields();
    if ( !ArrayUtils.isEmpty( outputFields ) ) {
      for ( int i = 0; i < outputFields.length; i++ ) {
        final TextFileField outputField = outputFields[ i ];
        fields.add( outputField.getName() );
      }
    } else {
      // if no output fields are specified, we include ALL input fields
      // get all input steps
      final Map<String, RowMetaInterface> inputRowMetaInterfaces = getInputRowMetaInterfaces( meta );
      if ( MapUtils.isNotEmpty( inputRowMetaInterfaces ) ) {
        for ( Map.Entry<String, RowMetaInterface> entry : inputRowMetaInterfaces.entrySet() ) {
          final RowMetaInterface inputFields = entry.getValue();
          if ( inputFields != null ) {
            for ( ValueMetaInterface valueMetaInterface : inputFields.getValueMetaList() ) {
              fields.add( valueMetaInterface.getName() );
            }
          }
        }
      }
    }

    return fields;
  }

  // used for unit testing
  protected void setObjectFactory( IMetaverseObjectFactory factory ) {
    this.metaverseObjectFactory = factory;
  }

}
