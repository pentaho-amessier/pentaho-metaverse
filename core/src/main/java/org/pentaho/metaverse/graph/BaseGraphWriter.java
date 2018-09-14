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

package org.pentaho.metaverse.graph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.IGraphWriter;
import org.pentaho.metaverse.api.model.BaseMetaverseBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A base implementation of the {@link IGraphWriter}.
 */
public abstract class BaseGraphWriter implements IGraphWriter {

  @Override
  public final void outputGraph( Graph graph, OutputStream graphMLOutputStream ) throws IOException {
    // TODO: check config to see if fields should be de-dupped
    deduplicateFields( graph );
    adjustCollectionFields( graph );
    outputGraphImpl( graph, graphMLOutputStream );
  }

  protected abstract void outputGraphImpl( final Graph graph, final OutputStream outputStream ) throws IOException;

  private Set<Vertex> getVertices( final Graph graph, final String category ) {
    final Iterator<Vertex> allVertices = graph.getVertices().iterator();

    final Set<Vertex> documentElementVertices = new HashSet();
    while ( allVertices.hasNext() ) {
      final Vertex vertex = allVertices.next();
      if ( category.equals( vertex.getProperty( DictionaryConst.PROPERTY_CATEGORY ) ) ) {
        documentElementVertices.add( vertex );
      }
    }
    return documentElementVertices;
  }

  /**
   * Returns all vertices categorized as "documentelement", which corresponds to all Step and Job Entry vertices.
   *
   * @param graph the {@link Graph}
   * @return a {@link Set} of vertices corresponding to Steps and Job Enties.
   */
  private Set<Vertex> getDocumentElementVertices( final Graph graph ) {
    return getVertices( graph, DictionaryConst.CATEGORY_DOCUMENT_ELEMENT );
  }

  private void deduplicateFields( final Graph graph ) {
    // get all Step and Job Entry nodes
    final Iterator<Vertex> documentElementVertices = getDocumentElementVertices( graph ).iterator();

    while ( documentElementVertices.hasNext() ) {
      final Vertex documentElementVertex = documentElementVertices.next();
      // merge fields at the end of the "inputs" edges
      //mergeFields( graph, documentElementVertex, Direction.IN, DictionaryConst.LINK_INPUTS, true );
      // merge fields at the end of the "outputs" edges
      mergeFields( graph, documentElementVertex, Direction.OUT, DictionaryConst.LINK_OUTPUTS, true );
    }
  }

  /*
     if ( DictionaryConst.CATEGORY_FIELD.equals( inputVertexCategory )
            && ( inputVertexType == null || !DictionaryConst.NODE_TYPE_TRANS_FIELD.equals( inputVertexType ) ) ) {
   */
  private void mergeFields( final Graph graph, final Vertex documentElementVertex, final Direction direction,
                            final String linkLabel, boolean isTransformationField ) {
    // get all edges corresponding to the requested direction and with the requested label
    final Iterator<Edge> links = documentElementVertex.getEdges( direction, linkLabel ).iterator();
    // traverse the links and see if there are any that point to fields with the same names, if so, they need to be
    // merged
    final Map<String, Set<Vertex>> fieldMap = new HashMap();
    while ( links.hasNext() ) {
      final Edge link = links.next();
      // get the vertex at the "other" end of this linnk ("this" end being the vertex itself)
      final Vertex vertex = link.getVertex( direction == Direction.IN ? Direction.OUT : Direction.IN );
      final String category = vertex.getProperty( DictionaryConst.PROPERTY_CATEGORY );
      final String type = vertex.getProperty( DictionaryConst.PROPERTY_TYPE );
      // verify that the vertex is a field of the desired type
      if ( DictionaryConst.CATEGORY_FIELD.equals( category )
        && isTransformationField == DictionaryConst.NODE_TYPE_TRANS_FIELD.equals( type ) ) {
        final String fieldName = vertex.getProperty( DictionaryConst.PROPERTY_NAME );

        Set<Vertex> fieldsWithSameName = fieldMap.get( fieldName );
        if ( fieldsWithSameName == null ) {
          fieldsWithSameName = new HashSet();
          fieldMap.put( fieldName, fieldsWithSameName );
        }
        fieldsWithSameName.add( vertex );
      }
    }

    // traverse the map pf fields - for any field name, if more than one has been found, merge them into one
    final Iterator<Map.Entry<String, Set<Vertex>>> fieldIter = fieldMap.entrySet().iterator();
    while ( fieldIter.hasNext() ) {
      final Map.Entry<String, Set<Vertex>> fieldEntry = fieldIter.next();
      final List<Vertex> fieldVertices = new ArrayList( fieldEntry.getValue() );
      if ( fieldVertices.size() > 1 ) {
        // get the first vertex - we will keep this one and re-point links connected to all the rest back to this
        // original one, and then remove the remaining ones
        final Vertex fieldVertexToKeep = fieldVertices.get( 0 );
        for ( int i = 1; i < fieldVertices.size(); i++ ) {
          final Vertex fieldVertexToMerge = fieldVertices.get( i );
          rewireEdges( graph, fieldVertexToKeep, fieldVertexToMerge, Direction.IN );
          rewireEdges( graph, fieldVertexToKeep, fieldVertexToMerge, Direction.OUT );
          // we can now safely remove 'fieldVertexToMerge'
          fieldVertexToMerge.remove();
        }
      }
    }
  }

  private void rewireEdges( final Graph graph, final Vertex vertexToKeep, final Vertex vertexToMerge,
                            final Direction direction ) {
    final Iterator<Edge> links = vertexToMerge.getEdges( direction ).iterator();
    while ( links.hasNext() ) {
      // remove this edge and recreate one that points to 'fieldVertexToKeep' instead of 'fieldVertexToMerge',
      // where it pointed originally
      final Edge originalLink = links.next();
      final String newLinkId = direction == Direction.OUT
        ? BaseMetaverseBuilder.getEdgeId(
        vertexToKeep, originalLink.getLabel(), originalLink.getVertex( Direction.IN ) )
        : BaseMetaverseBuilder.getEdgeId(
        originalLink.getVertex( Direction.OUT ), originalLink.getLabel(), vertexToKeep );
      if ( graph.getEdge( newLinkId ) == null ) {
        if ( direction == Direction.OUT ) {
          graph.addEdge( newLinkId, vertexToKeep, originalLink.getVertex( Direction.IN ), originalLink.getLabel() )
            .setProperty( "text", originalLink.getLabel() );
        } else {
          graph.addEdge( newLinkId, originalLink.getVertex( Direction.OUT ), vertexToKeep,
            originalLink.getLabel() ).setProperty( "text", originalLink.getLabel() );
        }
      }
      // remove the original link
      originalLink.remove();
    }
  }

  private Set<Vertex> getCollectionVertices( final Graph graph ) {
    return getVertices( graph, DictionaryConst.CATEGORY_FIELD_COLLECTION );
  }

  private void adjustCollectionFields( final Graph graph ) {

    // first add the missing links from each Collection to its fields
    addCollectionOutputFieldsLinks( graph );
    // if a single step reads more than one Collection, we will now likely have duplicate fields "output" by each
    // collection, which need to be de-duplicated
    deduplicateCollectionFields( graph );

    addCollectionFieldInputLinks( graph );
  }

  private List<Vertex> getLinkedVertices( final Vertex originVertex, final Direction edgeDirection,
                                          final String edgeLabel,
                                          final String linkedVertexCategory, final boolean equalToCategory,
                                          final String linkedVertexType, final boolean equalToType ) {

    final List<Vertex> linkedVertices = new ArrayList();
    final Iterator<Edge> links = originVertex.getEdges( edgeDirection, edgeLabel ).iterator();
    while ( links.hasNext() ) {
      final Edge link = links.next();
      // get the vertex at the opposite end of the edge
      final Vertex vertex = link.getVertex( edgeDirection == Direction.IN ? Direction.OUT : Direction.IN );
      final String category = vertex.getProperty( DictionaryConst.PROPERTY_CATEGORY );
      final String type = vertex.getProperty( DictionaryConst.PROPERTY_TYPE );
      if ( ( linkedVertexCategory == null
          || ( category != null && equalToCategory == category.equals( linkedVertexCategory ) ) )
        && ( linkedVertexType == null
          || ( type != null && equalToType == type.equals( linkedVertexType ) ) ) ) {
        linkedVertices.add( vertex );
      }
    }
    return linkedVertices;
  }


  private List<Vertex> getCollectionOutputFields( final Vertex collectionVertex ) {
    return getLinkedVertices( collectionVertex, Direction.OUT,
      DictionaryConst.LINK_OUTPUTS, DictionaryConst.CATEGORY_FIELD, true,
      DictionaryConst.NODE_TYPE_TRANS_FIELD, false );
  }

  private List<Vertex> getStepOutputTransformationFields( final Vertex stepVertex ) {
    return getLinkedVertices( stepVertex, Direction.OUT,
      DictionaryConst.LINK_OUTPUTS, DictionaryConst.CATEGORY_FIELD, true,
      DictionaryConst.NODE_TYPE_TRANS_FIELD, true );
  }

  private List<Vertex> getStepInputCollectionFields( final Vertex stepVertex ) {
    return  getLinkedVertices( stepVertex, Direction.IN,
      DictionaryConst.LINK_INPUTS, DictionaryConst.CATEGORY_FIELD, true,
      DictionaryConst.NODE_TYPE_TRANS_FIELD, false );
  }

  private List<Vertex> getStepsReadingCollection( final Vertex collectionVertex ) {
    return getLinkedVertices( collectionVertex, Direction.OUT,
      DictionaryConst.LINK_READBY, DictionaryConst.CATEGORY_DOCUMENT_ELEMENT, true, null, false );
  }


  private List<String> getVertexNames( List<Vertex> vertices ) {
    return getVertexProperties( vertices, DictionaryConst.PROPERTY_NAME );
  }

  private List<String> getVertexProperties( List<Vertex> vertices, final String propertyName ) {
    List<String> vertexProperties = new ArrayList();
    for ( final Vertex vertex : vertices ) {
      vertexProperties.add( vertex.getProperty( propertyName ) );
    }
    return vertexProperties;
  }

  // get all collections
  // for each collection get its output fields
  // for each collection get the step that reads it
  // for each such step, check the input fields against the collection's output fields
  // add any missing inputs links
  private void addCollectionFieldInputLinks( final Graph graph ) {
    // get all Collections ( files, database tables etc... )
    final Iterator<Vertex> collectionVertices = getCollectionVertices( graph ).iterator();
    while ( collectionVertices.hasNext() ) {
      final Vertex collectionVertex = collectionVertices.next();

      // for each collection get its output fields
      final List<Vertex> collectionFieldVertices = getCollectionOutputFields( collectionVertex );

      // for each collection get the steps that read it
      final List<Vertex> stepVertices = getStepsReadingCollection( collectionVertex );

      // for each step, check if it contains "inputs" links from all the collection output fields, and if not, add
      // the missing links, as well as "populates" links to all step output fields with the same name
      for ( final Vertex stepVertex : stepVertices ) {
        final List<Vertex> stepOutputFieldVertices = getStepOutputTransformationFields( stepVertex );

        for ( final Vertex collectionFieldVertex : collectionFieldVertices ) {
          final String collectionFieldName = collectionFieldVertex.getProperty( DictionaryConst.PROPERTY_NAME );
          // add the missing "input" link from the collection field to the step
          final String inputLinkId = BaseMetaverseBuilder.getEdgeId(
            collectionFieldVertex, DictionaryConst.LINK_INPUTS, stepVertex );
          if ( graph.getEdge( inputLinkId ) == null ) {
            graph.addEdge( inputLinkId, collectionFieldVertex, stepVertex, DictionaryConst.LINK_INPUTS )
              .setProperty( "text", DictionaryConst.LINK_INPUTS );

            final Vertex stepOutputField = findVertexByName( stepOutputFieldVertices, collectionFieldName );
            // also add a "populates" link from the collection field to the step output field with the same name
            final String populatesLinkId = BaseMetaverseBuilder.getEdgeId(
              collectionFieldVertex, DictionaryConst.LINK_POPULATES, stepOutputField );
            if ( graph.getEdge( populatesLinkId ) == null ) {
              graph.addEdge( populatesLinkId, collectionFieldVertex, stepOutputField, DictionaryConst.LINK_POPULATES )
                .setProperty( "text", DictionaryConst.LINK_POPULATES );
            }
          }
        }
      }
    }
  }

  private Vertex findVertexByName( final List<Vertex> vertices, final String name ) {
    for ( final Vertex vertex : vertices ) {
      final String vertexName = vertex.getProperty( DictionaryConst.PROPERTY_NAME );
      if ( vertexName != null && name != null && vertexName.equals( name ) ) {
        return vertex;
      }
    }
    return null;
  }

  // add output field only to the file field which inputs into the step, also add inputs field to all steps which read
  // the file
  private void addCollectionOutputFieldsLinks( final Graph graph ) {
    // get all Collections (files, database tables etc...)
    Iterator<Vertex> collectionVertices = getCollectionVertices( graph ).iterator();

    while ( collectionVertices.hasNext() ) {
      final Vertex collectionVertex = collectionVertices.next();
      // keep track of field names, as we are adding them, so we don't add output edges to duplicate fields
      final Set<String> collectionFieldNames = new HashSet();
      // for each collection vertex, get all steps that read it
      final List<Vertex> stepVertices = getStepsReadingCollection( collectionVertex );
      for ( final Vertex stepVertex : stepVertices ) {

        // for each step, get all non-transformation fields linked through the IN "inputs" edges
        final List<Vertex> fieldVertices = getStepInputCollectionFields( stepVertex );
        for ( final Vertex fieldVertex : fieldVertices ) {
          final String fieldName = fieldVertex.getProperty( DictionaryConst.PROPERTY_NAME );
          final String newLinkId = BaseMetaverseBuilder.getEdgeId(
            collectionVertex, DictionaryConst.LINK_OUTPUTS, fieldVertex );
          if ( graph.getEdge( newLinkId ) == null  && !collectionFieldNames.contains( fieldName ) ) {
            //collectionFieldNames.add( fieldName );
            // add an "outputs" edge from the collection to the field, but only if an edge to a field with that
            // name doesn't already exist
            graph.addEdge( newLinkId, collectionVertex, fieldVertex, DictionaryConst.LINK_OUTPUTS )
              .setProperty( "text", DictionaryConst.LINK_OUTPUTS );
          }
        }
        // for each step, get all IN "inputs" edges to get the input fields
        final Iterator<Edge> stepInputLinks = stepVertex.getEdges(
          Direction.IN, DictionaryConst.LINK_INPUTS ).iterator();
        while ( stepInputLinks.hasNext() ) {
          final Edge stepInputLink = stepInputLinks.next();
          final Vertex inputVertex = stepInputLink.getVertex( Direction.OUT );
          final String inputVertexCategory = inputVertex.getProperty( DictionaryConst.PROPERTY_CATEGORY );
          final String inputVertexType = inputVertex.getProperty( DictionaryConst.PROPERTY_TYPE );
          final String inputVertexName = inputVertex.getProperty( DictionaryConst.PROPERTY_NAME );
          // consider any field whose category is NOT "Transformation Stream Field", this will leave out all the
          // fields specifically related to the collection (File Fiels, Database Column etc...)
          if ( DictionaryConst.CATEGORY_FIELD.equals( inputVertexCategory )
            && ( inputVertexType == null || !DictionaryConst.NODE_TYPE_TRANS_FIELD.equals( inputVertexType ) ) ) {
            final String newLinkId = BaseMetaverseBuilder.getEdgeId(
              collectionVertex, DictionaryConst.LINK_OUTPUTS, inputVertex );
            if ( graph.getEdge( newLinkId ) == null && !collectionFieldNames.contains( inputVertexName )) {
              collectionFieldNames.add( inputVertexName );
              // add an "outputs" edge from the collection to the field, but only if an edge to a field with that
              // name doesn't already exist
              graph.addEdge( newLinkId, collectionVertex,inputVertex, DictionaryConst.LINK_OUTPUTS )
                .setProperty( "text", DictionaryConst.LINK_OUTPUTS );
            }
          }
        }
      }
   }
  }

  private void deduplicateCollectionFields( final Graph graph ) {
    // get all Collections (files, database tables etc...)
    final Iterator<Vertex> collectionVertices = getCollectionVertices( graph ).iterator();

    // traverse the links and see if there are any that point to fields with the same names, if so, they need to be
    // merged
    final Map<String, Set<Vertex>> fieldMap = new HashMap();

    while ( collectionVertices.hasNext() ) {
      final Vertex collectionVertex = collectionVertices.next();
      // merge non-transformation fields at the end of the "outputs" edges
      mergeFields( graph, collectionVertex, Direction.OUT, DictionaryConst.LINK_OUTPUTS, false );
    }
  }
}
