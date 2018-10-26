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

package org.pentaho.metaverse.api.analyzer.kettle.step;

import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.metaverse.api.IClonableDocumentAnalyzer;
import org.pentaho.metaverse.api.IComponentDescriptor;

public interface IClonableStepAnalyzer<T extends BaseStepMeta> extends IStepAnalyzer<T> {

  /**
   * Clones this {@link IClonableStepAnalyzer}.
   *
   * @return a clone of this {@link IClonableStepAnalyzer}
   */
  IClonableStepAnalyzer cloneAnalyzer();

  void setDocumentAnalyzer( final IClonableDocumentAnalyzer parentTransformationAnalyser );

  void setDocumentDescriptor( final IComponentDescriptor documentDescriptor );

  void setDocumentPath( final String documentPath );
}
