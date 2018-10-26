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

package org.pentaho.metaverse.analyzer.kettle.step.mapping;

import com.google.common.collect.ImmutableMap;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mapping.MappingIODefinition;
import org.pentaho.di.trans.steps.mapping.MappingMeta;
import org.pentaho.di.trans.steps.mapping.MappingValueRename;
import org.pentaho.di.trans.steps.mappinginput.MappingInputMeta;
import org.pentaho.di.trans.steps.mappingoutput.MappingOutput;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.StepField;
import org.pentaho.metaverse.api.analyzer.kettle.KettleAnalyzerUtil;
import org.pentaho.metaverse.api.analyzer.kettle.step.StepAnalyzer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MappingAnalyzer extends StepAnalyzer<MappingMeta> {
  @Override
  public Set<Class<? extends BaseStepMeta>> getSupportedSteps() {
    Set<Class<? extends BaseStepMeta>> supported = new HashSet<>();
    supported.add( MappingMeta.class );
    return supported;
  }

  @Override
  protected Set<StepField> getUsedFields( final MappingMeta meta ) {
    final Set<StepField> usedFields = new HashSet();
    return usedFields;
  }

  @Override
  protected void customAnalyze( final MappingMeta meta, final IMetaverseNode rootNode )
    throws MetaverseAnalyzerException {

    final String transformationPath = parentTransMeta.environmentSubstitute( meta.getFileName() );
    rootNode.setProperty( "subTransformation", transformationPath );

    final TransMeta subTransMeta = KettleAnalyzerUtil.getSubTransMeta( meta );
    subTransMeta.setFilename( transformationPath );

    final IMetaverseNode subTransNode = getNode( subTransMeta.getName(), DictionaryConst.NODE_TYPE_TRANS,
      descriptor.getNamespace(), null, null );
    subTransNode.setProperty( DictionaryConst.PROPERTY_PATH,
      KettleAnalyzerUtil.getSubTransMetaPath( meta, subTransMeta ) );
    subTransNode.setLogicalIdGenerator( DictionaryConst.LOGICAL_ID_GENERATOR_DOCUMENT );

    metaverseBuilder.addLink( rootNode, DictionaryConst.LINK_EXECUTES, subTransNode );

    // analyze the sub-transformation
    documentAnalyzer.analyze( documentDescriptor, subTransMeta, subTransNode,
      KettleAnalyzerUtil.getSubTransMetaPath( meta, subTransMeta ) );

    // ----------- analyze inputs, for each input:
    // - create a virtual step node for the input source step, the step name is either defined "input source step
    //   name", or if "main data path" is selected, it is the first encountered input step into the mapping step
    // - create a virtual step node for the mapping target step, the step name is defined in "mapping target step name",
    //   or if "main data path" is selected we need fake it since we will not have access to it, name it something
    //   like: [<sub-transformation>]: mapping input specification step name
    // - create virtual field nodes for all fields defined in the "fieldname to mapping input step" column and
    //   "derives" links from the source step fields defined in the "fieldname from source step" column that belong
    //   to the input source steps to these new virtual fields
    final List<String> verboseProps = new ArrayList();
    processInputMappings( subTransMeta, verboseProps, meta.getInputMappings() );

    // ----------- analyze outputs, for each output:
    // - create a virtual step node for the mapping source step, the step name is either defined "mapping source
    //   step name, or if "main data path" is selected we need fake it since we will not have access to it, name it
    //   something like: [<sub-transformation>]: mapping output specification step name
    // - create a virtual step node for the output target step, the step name is defined in "output target step name",
    //   or if "main data path" is is the first output step encountered that the mapping step outputs into
    // - create virtual field nodes for all fields defined in the "fieldname from mapping step" column and
    //   "derives" links from these new virtual fields to the fields defined in the "fieldsname to target step" that
    //   belong to the output target step
    processOutputMappings( subTransMeta, verboseProps, meta.getOutputMappings() );

    // When "Update mapped field names" is selected, the name of the parent field is used, rather than the sub-trans
    // field and vice versa
    createLinks( subTransMeta, meta );
    rootNode.setProperty( DictionaryConst.PROPERTY_VERBOSE_DETAILS, StringUtils.join( verboseProps, "," ) );
  }

  private boolean shouldRenameFields( final MappingMeta meta ) {
    for ( final MappingIODefinition inputMapping : meta.getInputMappings() ) {
      // if ANY one of the mappings has the rename flag set, we are renaming fields
      if ( inputMapping.isRenamingOnOutput() ) {
        return true;
      }
    }
    return false;
  }

  private static final String MAPPING_INPUT_SPEC = BaseMessages.getString(
    MappingInputMeta.class, "MappingInputDialog.Shell.Title");

  private static final String MAPPING_OUTPUT_SPEC = BaseMessages.getString(
    MappingOutput.class, "MappingOutputDialog.Shell.Title");

  /*
  private Vertex findStepVertex( final TransMeta transMeta, final Map<String, String> properties,
                                     final Vertex linedVertex, final Direction direction, final String linkLabel ) {
    final List<Vertex> potentialMatches = findVertices( transMeta, properties );
    for ( final Vertex potentialMatch : potentialMatches ) {
      final Iterator<Vertex> linkedVertivces = potentialMatch.getVertices( direction, linkLabel ).iterator();
      while ( linkedVertivces.hasNext() ) {
        final Vertex hoppingVertex = linkedVertivces.next();
        if ( hoppingVertex.equals( linedVertex ) ) {
          return potentialMatch;
        }
      }
    }
    return null;
  }*/

  private Vertex getInputSourceStepVertex( final TransMeta transMeta, final MappingIODefinition mapping ) {
    // get the vertex corresponding to the input source step; if "Main data path" is selected, this it the input
    // step (assuming there is only one) into this mapping step; if "Main data path" is not selected, it is the
    // step defined within the "Input source step name" field
    String stepName = null;
    if ( mapping != null ) {
      if ( mapping.isMainDataPath() ) {
        // TODO: warn if more than one previous step, invalid configuration
        stepName = ArrayUtils.isEmpty( prevStepNames ) ? null : prevStepNames[ 0 ];
      } else {
        // main path is not selected, therefore we can further refine our search by looking up the step by the name
        // defined in the "Mapping target step name" field
        stepName = mapping.getInputStepname();
      }
    }
    return findStepVertex( transMeta, stepName );
  }

  private Vertex getInputTargetStepVertex( final TransMeta transMeta, final MappingIODefinition mapping ) {
    // find the "Mapping input specification" step within the sub-transformation - there might be more than one,
    // if we have more than one "Mapping (sub-transformationn)" step within this ktr, so we need to find the one
    // connected to this specific sub-transformation
    final Map<String, String> propsLookupMap = new HashMap();
    propsLookupMap.put( DictionaryConst.PROPERTY_STEP_TYPE, MAPPING_INPUT_SPEC );
    if ( mapping != null && !mapping.isMainDataPath() ) {
      // main path is not selected, therefore we can further refine our search by looking up the step by the name
      // defined in the "Mapping target step name" field
      propsLookupMap.put( DictionaryConst.PROPERTY_NAME, mapping.getOutputStepname() );
    }
    return findStepVertex( transMeta, propsLookupMap );
  }

  private Vertex getOutputSourceStepVertex( final TransMeta transMeta, final MappingIODefinition mapping ) {
    // find the "Mapping output specification" step within the sub-transformation - there might be more than one,
    // if we have more than one "Mapping (sub-transformationn)" step within this ktr, so we need to find the one
    // connected to this specific sub-transformation
    final Map<String, String> propsLookupMap = new HashMap();
    propsLookupMap.put( DictionaryConst.PROPERTY_STEP_TYPE, MAPPING_OUTPUT_SPEC );
    if ( mapping != null && !mapping.isMainDataPath() ) {
      // main path is not selected, therefore we can further refine our search by looking up the step by the name
      // defined in the "Mapping source step name" field
      propsLookupMap.put( DictionaryConst.PROPERTY_NAME, mapping.getInputStepname() );
    }
    return findStepVertex( transMeta, propsLookupMap );
  }

  private Vertex getOutputTargetStepVertex( final TransMeta transMeta, final MappingIODefinition mapping ) {
    // get the vertex corresponding to the output target step; if "Main data path" is selected, this it the output
    // step (assuming there is only one) from this mapping step; if "Main data path" is not selected, it is the
    // step defined within the "Output target step name" field
    final Map<String, String> propsLookupMap = new HashMap();
    propsLookupMap.put( DictionaryConst.PROPERTY_TYPE, DictionaryConst.NODE_TYPE_TRANS_STEP );
    if ( mapping != null && !mapping.isMainDataPath() ) {
      // main path is not selected, therefore we can further refine our search by looking up the step by the name
      // defined in the "Mapping target step name" field
      propsLookupMap.put( DictionaryConst.PROPERTY_NAME, mapping.getOutputStepname() );
    } else {
      // TODO: once we move this procesing to "postAnalyze", we won't need to work on transMeta hops directly, we'll
      // be able to look at the HOPS_TO links
      // we are using the main data path - that means we need to find the step that this step hops to; since the
      // "hops_to" links will not have been established yet for this step, we need to inspect the transMeta hops to
      // find the name of the step of intrest
      final int numHops = transMeta.nrTransHops();
      for ( int i = 0; i < numHops; i++ ) {
        final TransHopMeta hop = transMeta.getTransHop( i );
        final StepMeta fromStep = hop.getFromStep();
        if ( fromStep.getName().equalsIgnoreCase( rootNode.getName() ) ) {
          // we found a hop originating from this step, the source of this hop is the target node we are interested in
          propsLookupMap.put( DictionaryConst.PROPERTY_NAME, hop.getToStep().getName() );
        }
      }
    }
    return findStepVertex( transMeta, propsLookupMap );
  }

  private void createLinks( final TransMeta subTransMeta, final MappingMeta meta ) {
    // TODO: take this into account when creating "derives" links so the links are made between the right fields
    boolean renameFields = shouldRenameFields( meta );

    final Map<String, String> fieldRenames = new HashMap();

    for ( final MappingIODefinition inputMapping : meta.getInputMappings() ) {

      final Vertex inputSourceVertex = getInputSourceStepVertex( parentTransMeta, inputMapping );
      final Vertex inputTargetVertex = getInputTargetStepVertex( subTransMeta, inputMapping );
      final String inputTargetName = inputTargetVertex.getProperty( DictionaryConst.PROPERTY_NAME );

      final List<MappingValueRename> renames = inputMapping.getValueRenames();
      for ( final MappingValueRename rename : renames ) {
        // This is deliberately target > source, since we are going to be looking up the target to rename back to the
        // source
        fieldRenames.put( rename.getTargetValueName(), rename.getSourceValueName() );
      }

      // traverse output fields of the input source step
      // for each field, if a rename exists, create a "derives" link from the field to the input target field with
      // the name defined in the "Fieldname to mapping input step" column; otherwise create a "derives" link to a
      // field with the same name
      final Iterator<Vertex> inputSourceOutputFields = inputSourceVertex.getVertices( Direction.OUT,
        DictionaryConst.LINK_OUTPUTS ).iterator();
      while ( inputSourceOutputFields.hasNext() ) {
        final Vertex inputSourceOutputField = inputSourceOutputFields.next();
        final String inputSourceOutputFieldName = inputSourceOutputField.getProperty( DictionaryConst.PROPERTY_NAME );
        // is there a rename for this field?
        final MappingValueRename renameMapping = inputMapping.getValueRenames().stream().filter( rename ->
          inputSourceOutputFieldName.equals( rename.getSourceValueName() ) ).findAny().orElse( null );
        // if there is no rename for this field, we look for a field with the same name, otherwise we look for a field
        // that is the target of the rename mapping ( defined within the "Fieldname to mapping input step" column);
        // we look for this field within the sub-transformation and within the context of the input target step
        final String derivedFieldName = renameMapping == null ? inputSourceOutputFieldName
          : renameMapping.getTargetValueName();
        final Vertex derivedField = findFieldVertex( subTransMeta, inputTargetName, derivedFieldName );
        // add an "inputs" link from the field of the input source step to the input target step
        metaverseBuilder.addLink( inputSourceOutputField, DictionaryConst.LINK_INPUTS, inputTargetVertex );
        // add a "derives" link from the field of the input source step to the output field of the input target step
        metaverseBuilder.addLink( inputSourceOutputField, DictionaryConst.LINK_DERIVES, derivedField );
      }
    }

    for ( final MappingIODefinition outputMapping : meta.getOutputMappings() ) {

      final Vertex outputSourceVertex = getOutputSourceStepVertex( subTransMeta, outputMapping );
      final Vertex outputTargetVertex = getOutputTargetStepVertex( parentTransMeta, outputMapping );
      final String outputTargetName = outputTargetVertex.getProperty( DictionaryConst.PROPERTY_NAME );

      // traverse output fields of the output source step
      // for each field, if a rename exists, create a "derives" link from the field to the input target field with
      // the name defined in the "Fieldname to mapping input step" column; otherwise create a "derives" link to a
      // field with the same name
      final Iterator<Vertex> outputSourceFields = outputSourceVertex.getVertices( Direction.OUT,
        DictionaryConst.LINK_OUTPUTS ).iterator();

      while ( outputSourceFields.hasNext() ) {
        final Vertex outputSourceField = outputSourceFields.next();
        final String outputSourceFieldName = outputSourceField.getProperty( DictionaryConst.PROPERTY_NAME );
        String derivedFieldName = outputSourceFieldName;
        // is there a rename mapping for this field?
        final MappingValueRename renameMapping = outputMapping.getValueRenames().stream().filter( rename ->
          outputSourceFieldName.equals( rename.getSourceValueName() ) ).findAny().orElse( null );
        // if an output rename mapping exists for this field, use the target field name for the derived field
        if ( renameMapping != null ) {
          derivedFieldName = renameMapping.getTargetValueName();
        } else if ( renameFields && fieldRenames.containsKey( outputSourceFieldName ) ) {
          // if no rename mapping exists, check if the field is defined within fieldRenamed
          derivedFieldName = fieldRenames.get( outputSourceFieldName );
        }
        final Vertex derivedField = findFieldVertex( parentTransMeta, outputTargetName, derivedFieldName );
        // add an "inputs" link from the field of the input source step to the input target step
        metaverseBuilder.addLink( outputSourceField, DictionaryConst.LINK_INPUTS, outputTargetVertex );
        // add a "derives" link from the field of the output source step to the output field of the output target step
        metaverseBuilder.addLink( outputSourceField, DictionaryConst.LINK_DERIVES, derivedField );
        // TODO:
        // if we are renaming a field, it's likely that the outputTargetVertex has an input vertex correspnding to
        // the field name before the rename occurred - this field should me removed, as it is not needed
      }
    }
  }

  // TODO: redo this - we can now access the actual steps, even when main path is selected
  private void processInputMappings( final TransMeta subTransMeta, final List<String> verboseProps,
                                     final List<MappingIODefinition> inputMappings ) {
    int mappingIdx = 1;
    for ( final MappingIODefinition mapping : inputMappings ) {
      final String mappingKey = "input [" + mappingIdx + "]";
      verboseProps.add( mappingKey );
      String sourceStep = getSourceStepName( subTransMeta, mapping, true, true );
      String targetStep = getTargetStepName( subTransMeta, mapping, true, true );
      final StringBuilder mappingStr = new StringBuilder();
      if ( sourceStep != null && targetStep != null ) {
        mappingStr.append( sourceStep ).append( " > " ).append( targetStep );
      }
      // main path?
      if ( !mapping.isMainDataPath() ) {
        final String descriptionKey = mappingKey + " description";
        verboseProps.add( descriptionKey );
        rootNode.setProperty( descriptionKey, mapping.getDescription() );
      }
      setCommonProps( mappingKey, mapping, mappingStr, verboseProps );
      mappingIdx++;
    }
  }

  // TODO: redo this - we can now access the actual steps, even when main path is selected
  private void processOutputMappings( final TransMeta subTransMeta, final List<String> verboseProps,
                                      final List<MappingIODefinition> mappings ) {
    int mappingIdx = 1;
    for ( final MappingIODefinition mapping : mappings ) {
      final String mappingKey = "output [" + mappingIdx + "]";
      verboseProps.add( mappingKey );
      String sourceStep = getSourceStepName( subTransMeta, mapping, true, false );
      String targetStep = getTargetStepName( subTransMeta, mapping, true, false );
      final StringBuilder mappingStr = new StringBuilder();
      if ( sourceStep != null && targetStep != null ) {
        mappingStr.append( sourceStep ).append( " > " ).append( targetStep );
      }
      // main path?
      if ( !mapping.isMainDataPath() ) {
        final String descriptionKey = mappingKey + " description";
        verboseProps.add( descriptionKey );
        rootNode.setProperty( descriptionKey, mapping.getDescription() );
      }
      setCommonProps( mappingKey, mapping, mappingStr, verboseProps );
      mappingIdx++;
    }
  }

  private String getSourceStepName( final TransMeta subTransMeta, final MappingIODefinition mapping,
                                    final boolean includeSubTransPrefix, final boolean input ) {
    String sourceStep = null;
    if ( mapping != null ) {
      if ( input ) {
        sourceStep = mapping.isMainDataPath() ? ( prevStepNames.length > 0 ? prevStepNames[ 0 ] : null )
          : mapping.getInputStepname();
      } else {
        if ( mapping.isMainDataPath() ) {
          final List<TransHopMeta> hops = parentTransMeta.getTransHops();
          for ( final TransHopMeta hop : hops ) {
            if ( hop.getFromStep().equals( parentStepMeta ) ) {
              sourceStep = ( includeSubTransPrefix ? "[" + subTransMeta.getName() + "] " : "" )
                + "<mapping_output_specification>";
              break;
            }
          }
        } else {
          sourceStep = ( includeSubTransPrefix ? "[" + subTransMeta.getName() + "] " : "" )
            + mapping.getInputStepname();
        }
      }
    }
    return sourceStep;
  }

  private String getTargetStepName( final TransMeta subTransMeta, final MappingIODefinition mapping,
                                    final boolean includeSubTransPrefix, final boolean input ) {
    String targetStep = null;
    if ( mapping != null ) {
      if ( input ) {
        targetStep = ( includeSubTransPrefix ? "[" + subTransMeta.getName() + "] " : "" )
          + ( mapping.isMainDataPath() ? "<mapping_input_specification>" : mapping.getOutputStepname() );
      } else {
        if ( mapping.isMainDataPath() ) {
          final List<TransHopMeta> hops = parentTransMeta.getTransHops();
          for ( final TransHopMeta hop : hops ) {
            if ( hop.getFromStep().equals( parentStepMeta ) ) {
              targetStep = hop.getToStep().getName();
              break;
            }
          }
        } else {
          targetStep = mapping.getOutputStepname();
        }
      }
    }
    return targetStep;
  }

  public void setCommonProps( final String mappingKey, final MappingIODefinition mapping,
                              final StringBuilder mappingStr, final List<String> verboseProps ) {
    rootNode.setProperty( mappingKey, mappingStr.toString() );
    final String updateFieldnamesKey = mappingKey + " update field names";
    verboseProps.add( updateFieldnamesKey );
    rootNode.setProperty( updateFieldnamesKey, mapping.isRenamingOnOutput() );

    int renameIdx = 1;
    for ( final MappingValueRename valueRename : mapping.getValueRenames() ) {
      final String renameKey = mappingKey + " rename [" + renameIdx++ + "]";
      verboseProps.add( renameKey );
      rootNode.setProperty( renameKey, valueRename.getSourceValueName() + " > " + valueRename.getTargetValueName() );
    }
  }
}
