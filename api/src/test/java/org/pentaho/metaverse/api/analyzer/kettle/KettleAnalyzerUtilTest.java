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

package org.pentaho.metaverse.api.analyzer.kettle;

import org.apache.commons.vfs2.FileObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.IDocument;
import org.pentaho.metaverse.api.IMetaverseBuilder;
import org.pentaho.metaverse.api.INamespace;
import org.pentaho.metaverse.api.MetaverseException;
import org.pentaho.metaverse.api.Namespace;
import org.pentaho.metaverse.api.model.BaseMetaverseBuilder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * User: RFellows Date: 8/14/14
 */
public class KettleAnalyzerUtilTest {

  @Test
  public void testDefaultConstructor() {
    assertNotNull( new KettleAnalyzerUtil() );
  }

  @Test
  public void testNormalizeFilePath() throws Exception {
    String input;
    String expected;
    try {
      File f = File.createTempFile( "This is a text file", ".txt" );
      input = f.getAbsolutePath();
      expected = f.getAbsolutePath();
    } catch ( IOException ioe ) {
      // If this didn't work, we're running on a system where we can't create files, like CI perhaps.
      // In that case, use a RAM file. This test doesn't do much in that case, but it will pass.
      FileObject f = KettleVFS.createTempFile( "This is a text file", ".txt", "ram://" );
      input = f.getName().getPath();
      expected = f.getName().getPath();
    }

    String result = KettleAnalyzerUtil.normalizeFilePath( input );
    assertEquals( expected, result );

  }

  @Test
  public void tesBuildDocument() throws MetaverseException {
    final IMetaverseBuilder builder = new BaseMetaverseBuilder( null );
    final AbstractMeta transMeta = Mockito.mock( TransMeta.class );
    final String transName = "MyTransMeta";
    Mockito.doReturn( transName ).when( transMeta ).getName();
    Mockito.doReturn( "ktr" ).when( transMeta ).getDefaultExtension();
    final String id = "path.ktr";
    final String namespaceId = "MyNamespace";
    final INamespace namespace = new Namespace( namespaceId );

    assertNull( KettleAnalyzerUtil.buildDocument( null, transMeta, id, namespace ) );

    IDocument document = KettleAnalyzerUtil.buildDocument( builder, transMeta, id, namespace );
    assertNotNull( document );
    assertEquals( namespace, document.getNamespace() );
    assertEquals( transMeta, document.getContent() );
    assertEquals( id, document.getStringID() );
    assertEquals( transName, document.getName() );
    assertEquals( "ktr", document.getExtension() );
    assertEquals( DictionaryConst.CONTEXT_RUNTIME, document.getContext().getContextName() );
    assertEquals( document.getName(), document.getProperty( DictionaryConst.PROPERTY_NAME ) );
    assertEquals( KettleAnalyzerUtil.normalizeFilePath( "path.ktr" ), document.getProperty( DictionaryConst
      .PROPERTY_PATH ) );
    assertEquals(namespaceId, document.getProperty( DictionaryConst.PROPERTY_NAMESPACE ) );
  }
}
