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

package org.pentaho.metaverse.analyzer.kettle.step;

import org.junit.Test;
import org.pentaho.metaverse.api.analyzer.kettle.step.IClonableStepAnalyzer;

import static org.junit.Assert.assertNotEquals;

public abstract class ClonableAnalyzerTest {

  @Test
  public void testCloneAnalyser() {
    // verify that a call to cloneAnalyzer returns a different object than the original
    assertNotEquals( newInstance(), newInstance().cloneAnalyzer() );
  }

  protected abstract IClonableStepAnalyzer newInstance();
}
