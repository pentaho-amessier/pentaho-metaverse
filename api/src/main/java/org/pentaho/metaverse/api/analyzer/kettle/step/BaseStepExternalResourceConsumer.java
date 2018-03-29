/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.metaverse.api.analyzer.kettle.step;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.steps.file.BaseFileInputMeta;
import org.pentaho.di.trans.steps.file.BaseFileInputStep;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.AnalysisContext;
import org.pentaho.metaverse.api.IAnalysisContext;
import org.pentaho.metaverse.api.analyzer.kettle.KettleAnalyzerUtil;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class is a base implementation for StepExternalConsumer plugins. Subclasses should override the various methods
 * with business logic that can handle the external resources used by the given step.
 */
public abstract class BaseStepExternalResourceConsumer<S extends BaseStep, M extends BaseStepMeta>
  implements IStepExternalResourceConsumer<S, M> {

  private Map<String, Set<IExternalResourceInfo>> resourcesFromRow = new HashMap<>();

  protected boolean resolveExternalResources() {
    return true;
  }

  @Override
  public boolean isDataDriven( M meta ) {
    return false;
  }

  @Override
  public Collection<IExternalResourceInfo> getResourcesFromMeta( M meta ) {
    return getResourcesFromMeta( meta, new AnalysisContext( DictionaryConst.CONTEXT_RUNTIME ) );
  }

  @Override
  public Collection<IExternalResourceInfo> getResourcesFromMeta( final M meta, final IAnalysisContext context ) {
    Collection<IExternalResourceInfo> resources = null;
    if ( resolveExternalResources() && meta instanceof BaseFileInputMeta && !isDataDriven( meta ) ) {
      resources = KettleAnalyzerUtil.getResourcesFromMeta( (BaseFileInputMeta) meta, context );
    }
    return resources == null ? Collections.emptyList() : resources;
  }

  @Override
  public Collection<IExternalResourceInfo> getResources( final M meta, final IAnalysisContext context ) {
    Collection<IExternalResourceInfo> resources = null;
    // depending on whether we are data driven, we either return resources from row, or from meta
    if ( resolveExternalResources() ) {
      if ( isDataDriven( meta ) ) {
        final String stepName = meta.getParentStepMeta().getName();
        resources = this.resourcesFromRow.get( stepName );
      } else {
        resources = getResourcesFromMeta( meta, context );
      }
    }
    return resources == null ? Collections.emptyList() : resources;
  }

  @Override
  public Collection<IExternalResourceInfo> getResourcesFromRow(
    final S step, final RowMetaInterface rowMeta, final Object[] row ) {
    Collection<IExternalResourceInfo> resources = null;
    if ( resolveExternalResources() && step instanceof BaseFileInputStep ) {
      resources = KettleAnalyzerUtil.getResourcesFromRow((BaseFileInputStep) step, rowMeta, row );
      // keep track of resources from row, as they are encountered - we do this, because this method is called for
      // each row, and we need to keep track of all of them
      if ( !CollectionUtils.isEmpty( resources ) ) {
        addResourcesFromRow( step.getObjectName(), resources );
      }
    }
    return resources == null ? Collections.emptyList() : resources;
  }

  protected void addResourcesFromRow(
    final String stepName, final Collection<IExternalResourceInfo> resourcesFromRow ) {
    if ( !StringUtils.isBlank( stepName ) &&  !CollectionUtils.isEmpty( resourcesFromRow ) ) {
      Set<IExternalResourceInfo> rowResources = this.resourcesFromRow.get( stepName );
      if ( rowResources == null ) {
        rowResources = new HashSet<>();
        this.resourcesFromRow.put( stepName, rowResources );
      }
      for ( final IExternalResourceInfo resource : resourcesFromRow ) {
        rowResources.add( resource );
      }
    }
  }
}
