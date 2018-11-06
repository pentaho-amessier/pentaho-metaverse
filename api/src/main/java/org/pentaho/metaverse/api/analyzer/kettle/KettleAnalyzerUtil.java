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

package org.pentaho.metaverse.api.analyzer.kettle;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleMissingPluginsException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.ISubTransAwareMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.file.BaseFileInputMeta;
import org.pentaho.di.trans.steps.file.BaseFileInputStep;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.AnalysisContext;
import org.pentaho.metaverse.api.IComponentDescriptor;
import org.pentaho.metaverse.api.IDocument;
import org.pentaho.metaverse.api.IMetaverseBuilder;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.INamespace;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.MetaverseComponentDescriptor;
import org.pentaho.metaverse.api.MetaverseException;
import org.pentaho.metaverse.api.analyzer.kettle.step.StepAnalyzer;
import org.pentaho.metaverse.api.messages.Messages;
import org.pentaho.metaverse.api.model.ExternalResourceInfoFactory;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

public class KettleAnalyzerUtil {

  private static final Logger log = LoggerFactory.getLogger( KettleAnalyzerUtil.class );

  /**
   * Utility method for normalizing file paths used in Metaverse Id generation. It will convert a valid path into a
   * consistent path regardless of URI notation or filesystem absolute path.
   *
   * @param filePath full path to normalize
   * @return the normalized path
   */
  public static String normalizeFilePath( String filePath ) throws MetaverseException {
    try {
      String path = filePath;
      FileObject fo = KettleVFS.getFileObject( filePath );
      try {
        path = fo.getURL().getPath();
      } catch ( Throwable t ) {
        // Something went wrong with VFS, just try the filePath
      }
      File f = new File( path );
      return f.getAbsolutePath();
    } catch ( Exception e ) {
      throw new MetaverseException( e );
    }
  }

  public static String normalizeFilePathSafely( String filePath ) {
    try {
      return normalizeFilePath( filePath );
    } catch ( final MetaverseException e ) {
      log.error( e.getMessage() );
    }
    return "";
  }

  public static Collection<IExternalResourceInfo> getResourcesFromMeta(
    final StepMeta parentStepMeta, final String[] filePaths ) {
    Collection<IExternalResourceInfo> resources = Collections.emptyList();

    if ( parentStepMeta != null && filePaths != null && filePaths.length > 0 ) {
      resources = new ArrayList<>( filePaths.length );
      for ( final String path : filePaths ) {
        if ( !Const.isEmpty( path ) ) {
          try {

            final IExternalResourceInfo resource = ExternalResourceInfoFactory
              .createFileResource( KettleVFS.getFileObject( path ), true );
            if ( resource != null ) {
              resources.add( resource );
            } else {
              throw new KettleFileException( "Error getting file resource!" );
            }
          } catch ( KettleFileException kfe ) {
            // TODO throw or ignore?
          }
        }
      }
    }
    return resources;
  }

  public static Collection<IExternalResourceInfo> getResourcesFromRow(
    BaseFileInputStep step, RowMetaInterface rowMeta, Object[] row ) {

    Collection<IExternalResourceInfo> resources = new LinkedList<>();
    // For some reason the step doesn't return the StepMetaInterface directly, so go around it
    BaseFileInputMeta meta = (BaseFileInputMeta) step.getStepMetaInterface();
    if ( meta == null ) {
      meta = (BaseFileInputMeta) step.getStepMeta().getStepMetaInterface();
    }

    try {
      String filename = meta == null ? null : step.environmentSubstitute(
        rowMeta.getString( row, meta.getAcceptingField(), null ) );
      if ( !Const.isEmpty( filename ) ) {
        FileObject fileObject = KettleVFS.getFileObject( filename, step );
        resources.add( ExternalResourceInfoFactory.createFileResource( fileObject, true ) );
      }
    } catch ( KettleException kve ) {
      // TODO throw exception or ignore?
    }
    return resources;
  }

  public static TransMeta getSubTransMeta( final ISubTransAwareMeta meta ) throws MetaverseAnalyzerException {

    final TransMeta parentTransMeta = meta.getParentStepMeta().getParentTransMeta();
    final Repository repo = parentTransMeta.getRepository();

    TransMeta subTransMeta = null;
    switch ( meta.getSpecificationMethod() ) {
      case FILENAME:
        String transPath = null;
        try {
          transPath = KettleAnalyzerUtil.normalizeFilePath( parentTransMeta.environmentSubstitute(
            meta.getFileName() ) );
          subTransMeta = getSubTransMeta( transPath );
        } catch ( Exception e ) {
          throw new MetaverseAnalyzerException( "Sub transformation can not be found - " + transPath, e );
        }
        break;
      case REPOSITORY_BY_NAME:
        if ( repo != null ) {
          String dir = parentTransMeta.environmentSubstitute( meta.getDirectoryPath() );
          String file = parentTransMeta.environmentSubstitute( meta.getTransName() );
          try {
            RepositoryDirectoryInterface rdi = repo.findDirectory( dir );
            subTransMeta = repo.loadTransformation( file, rdi, null, true, null );
          } catch ( KettleException e ) {
            throw new MetaverseAnalyzerException( "Sub transformation can not be found in repository - " + file, e );
          }
        } else {
          throw new MetaverseAnalyzerException( "Not connected to a repository, can't get the transformation" );
        }
        break;
      case REPOSITORY_BY_REFERENCE:
        if ( repo != null ) {
          try {
            subTransMeta = repo.loadTransformation( meta.getTransObjectId(), null );
          } catch ( KettleException e ) {
            throw new MetaverseAnalyzerException( "Sub transformation can not be found by reference - "
              + meta.getTransObjectId(), e );
          }
        } else {
          throw new MetaverseAnalyzerException( "Not connected to a repository, can't get the transformation" );
        }
        break;
    }
    return subTransMeta;
  }


  public static String getSubTransMetaPath( final ISubTransAwareMeta meta, final TransMeta subTransMeta ) throws
    MetaverseAnalyzerException {

    final TransMeta parentTransMeta = meta.getParentStepMeta().getParentTransMeta();
    String transPath = null;
    switch ( meta.getSpecificationMethod() ) {
      case FILENAME:
        try {
          transPath = KettleAnalyzerUtil.normalizeFilePath( parentTransMeta.environmentSubstitute(
            meta.getFileName() ) );
        } catch ( Exception e ) {
          throw new MetaverseAnalyzerException( "Sub transformation can not be found - " + transPath, e );
        }
        break;
      case REPOSITORY_BY_NAME:
        transPath = subTransMeta.getPathAndName() + "." + subTransMeta.getDefaultExtension();
        break;
      case REPOSITORY_BY_REFERENCE:
        transPath = subTransMeta.getPathAndName() + "." + subTransMeta.getDefaultExtension();
        break;
    }
    try {
      transPath = KettleAnalyzerUtil.normalizeFilePath( transPath );
    } catch ( final MetaverseException e ) {
      // ignore
    }
    return transPath;
  }

  private static TransMeta getSubTransMeta( final String filePath ) throws FileNotFoundException, KettleXMLException,
    KettleMissingPluginsException {
    FileInputStream fis = new FileInputStream( filePath );
    return new TransMeta( fis, null, true, null, null );
  }

  /**
   * Builds a {@link IDocument} given the provided details.
   *
   * @param builder   the {@link IMetaverseBuilder}
   * @param meta      the {@link AbstractMeta} (trans or job)
   * @param id        the meta id (filename)
   * @param namespace the {@link INamespace}
   * @return a new {@link IDocument}
   */
  public static IDocument buildDocument( final IMetaverseBuilder builder, final AbstractMeta meta,
                                         final String id, final INamespace namespace ) {

    if ( builder == null || builder.getMetaverseObjectFactory() == null ) {
      log.warn( Messages.getString( "WARN.UnableToBuildDocument" ) );
      return null;
    }

    final IDocument metaverseDocument = builder.getMetaverseObjectFactory().createDocumentObject();
    if ( metaverseDocument == null ) {
      log.warn( Messages.getString( "WARN.UnableToBuildDocument" ) );
      return null;
    }

    metaverseDocument.setNamespace( namespace );
    metaverseDocument.setContent( meta );
    metaverseDocument.setStringID( id );
    metaverseDocument.setName( meta.getName() );
    metaverseDocument.setExtension( meta.getDefaultExtension() );
    metaverseDocument.setMimeType( URLConnection.getFileNameMap().getContentTypeFor(
      meta instanceof TransMeta ? "trans.ktr" : "job.kjb" ) );
    metaverseDocument.setContext( new AnalysisContext( DictionaryConst.CONTEXT_RUNTIME ) );
    String normalizedPath;
    try {
      normalizedPath = KettleAnalyzerUtil.normalizeFilePath( id );
    } catch ( MetaverseException e ) {
      normalizedPath = id;
    }
    metaverseDocument.setProperty( DictionaryConst.PROPERTY_NAME, meta.getName() );
    metaverseDocument.setProperty( DictionaryConst.PROPERTY_PATH, normalizedPath );
    metaverseDocument.setProperty( DictionaryConst.PROPERTY_NAMESPACE, namespace.getNamespaceId() );

    return metaverseDocument;
  }

  public static String getFilename( TransMeta transMeta ) {
    String filename = transMeta.getFilename();
    if ( filename == null ) {
      filename = transMeta.getPathAndName();
      if ( transMeta.getDefaultExtension() != null ) {
        filename = filename + "." + transMeta.getDefaultExtension();
      }
    }
    return filename;
  }

  /**
   * Analyzes in {@link StepAnalyzer} that is sub-transformation aware}
   *
   * @param analyzer  the {@link StepAnalyzer}
   * @param transMeta the step's parent {@link TransMeta}
   * @param meta      the step {@link ISubTransAwareMeta}
   * @param rootNode  the {@link IMetaverseNode}
   * @throws MetaverseAnalyzerException
   */
  public static void analyze( final StepAnalyzer analyzer, final TransMeta transMeta, final ISubTransAwareMeta meta,
                              final IMetaverseNode rootNode )
    throws MetaverseAnalyzerException {

    String transformationPath = KettleAnalyzerUtil.normalizeFilePathSafely(
      transMeta.environmentSubstitute( meta.getFileName() ) );
    rootNode.setProperty( "subTransformation", transformationPath );

    final TransMeta subTransMeta = KettleAnalyzerUtil.getSubTransMeta( meta );
    subTransMeta.setFilename( transformationPath );

    final IMetaverseNode subTransNode = analyzer.getNode( subTransMeta.getName(), DictionaryConst.NODE_TYPE_TRANS,
      analyzer.getDocumentDescriptor().getNamespace(), null, null );
    subTransNode.setProperty( DictionaryConst.PROPERTY_NAMESPACE,
      analyzer.getDocumentDescriptor().getNamespace().getNamespaceId() );
    subTransNode.setProperty( DictionaryConst.PROPERTY_PATH,
      KettleAnalyzerUtil.getSubTransMetaPath( meta, subTransMeta ) );
    subTransNode.setLogicalIdGenerator( DictionaryConst.LOGICAL_ID_GENERATOR_DOCUMENT );

    analyzer.getMetaverseBuilder().addLink( rootNode, DictionaryConst.LINK_EXECUTES, subTransNode );

    final String id = getFilename( subTransMeta );
    final IDocument subTransDocument = buildDocument( analyzer.getMetaverseBuilder(), subTransMeta,
      id, analyzer.getDocumentDescriptor().getNamespace() );
    final IComponentDescriptor subtransDocumentDescriptor = new MetaverseComponentDescriptor(
      subTransDocument.getStringID(), DictionaryConst.NODE_TYPE_TRANS, analyzer.getDocumentDescriptor().getNamespace(),
      analyzer.getDescriptor().getContext() );

    // analyze the sub-transformation
    analyzer.getDocumentAnalyzer().analyze( subtransDocumentDescriptor, subTransMeta, subTransNode,
      KettleAnalyzerUtil.getSubTransMetaPath( meta, subTransMeta ) );
  }

}
