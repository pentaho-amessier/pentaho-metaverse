/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.metaverse.util;

import com.pentaho.dictionary.DictionaryConst;
import com.pentaho.metaverse.api.IDocumentController;
import com.pentaho.metaverse.graph.LineageGraphCompletionService;
import com.pentaho.metaverse.graph.LineageGraphMap;
import com.pentaho.metaverse.impl.DocumentController;
import com.pentaho.metaverse.impl.MetaverseBuilder;
import com.pentaho.metaverse.impl.MetaverseComponentDescriptor;
import com.pentaho.metaverse.impl.MetaverseObjectFactory;
import com.pentaho.metaverse.messages.Messages;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.pentaho.platform.api.engine.IPentahoObjectFactory;
import org.pentaho.platform.api.metaverse.IDocument;
import org.pentaho.platform.api.metaverse.IDocumentAnalyzer;
import org.pentaho.platform.api.metaverse.IMetaverseBuilder;
import org.pentaho.platform.api.metaverse.IMetaverseObjectFactory;
import org.pentaho.platform.api.metaverse.INamespace;
import org.pentaho.platform.api.metaverse.IRequiresMetaverseBuilder;
import org.pentaho.platform.api.metaverse.MetaverseAnalyzerException;
import org.pentaho.platform.api.metaverse.MetaverseException;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.engine.core.system.objfac.StandaloneSpringPentahoObjectFactory;

import java.util.Set;
import java.util.concurrent.Future;


/**
 * A class containing utility methods for Metaverse / lineage
 */
public class MetaverseUtil {

  protected static IDocumentController documentController = null;

  /**
   * Creates a Spring object factory using the given config file, and injects the bean definitions therein.
   *
   * @param configPath the path of the plugin/solution. The config file should be located under this path at
   *                   /system/plugin.spring.xml
   */
  public static synchronized void initializeMetaverseObjects( String configPath ) throws MetaverseException {

    if ( configPath == null ) {
      throw new MetaverseException( Messages.getString( "ERROR.MetaverseInit.BadConfigPath", configPath ) );
    }

    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();

      Thread.currentThread().setContextClassLoader( MetaverseUtil.class.getClassLoader() );
      IPentahoObjectFactory pentahoObjectFactory = new StandaloneSpringPentahoObjectFactory();
      pentahoObjectFactory.init( configPath, PentahoSystem.getApplicationContext() );
      PentahoSystem.registerObjectFactory( pentahoObjectFactory );

      // Restore context classloader
      Thread.currentThread().setContextClassLoader( cl );
    } catch ( Exception e ) {
      throw new MetaverseException( Messages.getString( "ERROR.MetaverseInit.CouldNotInit" ), e );
    }
  }

  public static IDocumentController getDocumentController() {
    if ( documentController != null ) {
      return documentController;
    }
    try {
      documentController = PentahoSystem.get( IDocumentController.class );
      if ( documentController == null ) {
        documentController = new DocumentController();
      }
    } catch ( Exception e ) {
      documentController = new DocumentController();
    }
    return documentController;
  }

  public static IDocument createDocument(
    IMetaverseObjectFactory objectFactory,
    INamespace namespace,
    Object content,
    String id,
    String name,
    String extension,
    String mimeType ) {

    if ( objectFactory == null ) {
      objectFactory = new MetaverseObjectFactory();
    }
    IDocument metaverseDocument = objectFactory.createDocumentObject();

    metaverseDocument.setNamespace( namespace );
    metaverseDocument.setContent( content );
    metaverseDocument.setStringID( id );
    metaverseDocument.setName( name );
    metaverseDocument.setExtension( extension );
    metaverseDocument.setMimeType( mimeType );
    metaverseDocument.setProperty( DictionaryConst.PROPERTY_PATH, id );
    metaverseDocument.setProperty( DictionaryConst.PROPERTY_NAMESPACE, namespace.getNamespaceId() );

    return metaverseDocument;
  }

  public static void addLineageGraph( final IDocument document, Graph graph ) throws MetaverseException {

    if ( document == null ) {
      throw new MetaverseException( Messages.getString( "ERROR.Document.IsNull" ) );
    }

    // Find the transformation analyzer(s) and create Futures to analyze the transformation.
    // Right now we expect a single transformation analyzer in the system. If we need to support more,
    // the lineageGraphMap needs to map the TransMeta to a collection of Futures, etc.
    IDocumentController docController = MetaverseUtil.getDocumentController();
    if ( docController != null && docController instanceof IRequiresMetaverseBuilder ) {

      // Create a new builder, setting it on the DocumentController if possible
      IMetaverseBuilder metaverseBuilder = new MetaverseBuilder( graph );

      ( (IRequiresMetaverseBuilder) docController ).setMetaverseBuilder( metaverseBuilder );
      Set<IDocumentAnalyzer> matchingAnalyzers = docController.getDocumentAnalyzers( "ktr" );

      if ( matchingAnalyzers != null ) {
        for ( final IDocumentAnalyzer analyzer : matchingAnalyzers ) {

          Runnable analyzerRunner = new Runnable() {
            @Override
            public void run() {
              try {

                analyzer.analyze(
                  new MetaverseComponentDescriptor(
                    document.getName(),
                    DictionaryConst.NODE_TYPE_TRANS,
                    document.getNamespace() ),
                  document
                );
              } catch ( MetaverseAnalyzerException mae ) {
                throw new RuntimeException( Messages.getString( "ERROR.AnalyzingDocument", document.getNamespaceId() ), mae );
              }
            }
          };

          Graph g = ( graph != null ) ? graph : new TinkerGraph();
          Future<Graph> transAnalysis =
            LineageGraphCompletionService.getInstance().submit( analyzerRunner, g );

          // Save this Future, the client will call it when the analysis is needed
          LineageGraphMap.getInstance().put( document.getContent(), transAnalysis );
        }

      }
    }
  }
}