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

package org.pentaho.metaverse.api;

import java.util.Set;

/**
 * The IDocumentAnalyzer interface represents an object capable of analyzing certain types of documents.
 * 
 */
public interface IDocumentAnalyzer<S> extends IAnalyzer<S, IDocument> {

  /**
   * Gets the types of documents supported by this analyzer
   * 
   * @return the supported types
   */
  Set<String> getSupportedTypes();

  /**
   * Sets the {@link org.pentaho.di.job.Job} or {@link org.pentaho.di.trans.Trans} object associated within this
   * analyzer.
   * @param executable {@link org.pentaho.di.job.Job} or {@link org.pentaho.di.trans.Trans}
   */
  void setExecutable( final Object executable );

  /**
   * Returns the {@link org.pentaho.di.job.Job} or {@link org.pentaho.di.trans.Trans} object associated within this
   * analyzer.
   * @return the {@link org.pentaho.di.job.Job} or {@link org.pentaho.di.trans.Trans} object associated within this
   * analyzer.
   */
  Object getExecutable();
}
