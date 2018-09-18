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

package org.pentaho.metaverse.analyzer.kettle.step.tableinput;

import com.sun.xml.rpc.processor.modeler.j2ee.xml.string;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.util.DatabaseUtil;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.MetaverseComponentDescriptor;
import org.pentaho.metaverse.api.StepField;
import org.pentaho.metaverse.api.analyzer.kettle.step.ConnectionExternalResourceStepAnalyzer;
import org.pentaho.metaverse.api.model.BaseDatabaseResourceInfo;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The TableInputStepAnalyzer is responsible for providing nodes and links (i.e. relationships) between itself and other
 * metaverse entities
 */
public class TableInputStepAnalyzer extends ConnectionExternalResourceStepAnalyzer<TableInputMeta> {
  private Logger log = LoggerFactory.getLogger( TableInputStepAnalyzer.class );

  @Override
  public Set<Class<? extends BaseStepMeta>> getSupportedSteps() {
    return new HashSet<Class<? extends BaseStepMeta>>() {
      {
        add( TableInputMeta.class );
      }
    };
  }

  @Override
  protected IMetaverseNode[] createTableNodes( IExternalResourceInfo resource )
    throws MetaverseAnalyzerException {

    final List<IMetaverseNode> nodes = new ArrayList();
    try {
      final String sql = parentTransMeta.environmentSubstitute( baseStepMeta.getSQL() ).toLowerCase();
      String tablesStr = sql.substring( sql.indexOf( "from" ) + "from".length() );
      if ( tablesStr.indexOf( "where " ) > -1 ) {
        tablesStr = tablesStr.substring( 0, tablesStr.indexOf( "where " ) );
      }
      if ( tablesStr.indexOf( "order by " ) > -1 ) {
        tablesStr = tablesStr.substring( 0, tablesStr.indexOf( "order by " ) );
      }

      String[] tableNames = tablesStr.split( "," );
      for ( final String tableName : tableNames ) {
        nodes.add( createTableNode( resource, tableName.trim(), DictionaryConst.NODE_TYPE_DATA_TABLE ) );
      }
    }
    catch ( final Exception e ) {
      // TODO: log
    }
    return nodes.toArray( new IMetaverseNode[nodes.size()] );
  }

  @Override
  protected IMetaverseNode createTableNode( IExternalResourceInfo resource ) throws MetaverseAnalyzerException {
    return createTableNode( resource, DictionaryConst.NODE_NAME_SQL, DictionaryConst.NODE_TYPE_SQL_QUERY );
  }

  private IMetaverseNode createTableNode( final IExternalResourceInfo resource, final String tableName,
                                          final String nodeType ) throws
    MetaverseAnalyzerException {
    BaseDatabaseResourceInfo resourceInfo = (BaseDatabaseResourceInfo) resource;

    Object obj = resourceInfo.getAttributes().get( DictionaryConst.PROPERTY_QUERY );
    String query = obj == null ? null : obj.toString();

    // create a node for the table
    MetaverseComponentDescriptor componentDescriptor = new MetaverseComponentDescriptor(
      tableName,
      nodeType,
      getConnectionNode(),
      getDescriptor().getContext() );

    // set the namespace to be the id of the connection node.
    IMetaverseNode tableNode = createNodeFromDescriptor( componentDescriptor );
    tableNode.setProperty( DictionaryConst.PROPERTY_NAMESPACE, componentDescriptor.getNamespace().getNamespaceId() );
    tableNode.setProperty( DictionaryConst.PROPERTY_QUERY, query );
    tableNode.setLogicalIdGenerator( DictionaryConst.LOGICAL_ID_GENERATOR_DB_QUERY );
    return tableNode;
  }

  @Override
  protected boolean hasColumn( final String tableName, final String columnName ) {
    return getColumnNames( tableName ).contains( columnName.toLowerCase() );
  }

  private List<String> getSchemas( final Database db, final String tableName ) {
    final List<String> schemas = new ArrayList();
    if ( tableName.indexOf( '.' ) > 0 ) {
      schemas.add( tableName.substring( 0, tableName.indexOf( '.' ) ) );
    } else {
      try {
        final ResultSet rs = db.getConnection().getMetaData().getSchemas();
        while ( rs.next() ) {
          schemas.add( rs.getString( 1 ) );
        }
        rs.close();
      }
      catch ( final Exception e ) {

      }
    }
    return schemas;
  }
  private List<String> getColumnNames( final String tableName ) {

    final List<String> columnNames = new ArrayList();
    try {
      final Database db = new Database( null, baseStepMeta.getDatabaseMeta() );
      db.normalConnect( null );
      final List<String> schemas = getSchemas( db, tableName );
      for ( final String schema : schemas ) {
        final ResultSet rs = db.getColumnsMetaData( schema, tableName );
        while ( rs.next() ) {
          columnNames.add( rs.getString( 4 ).toLowerCase() );
        }
        rs.close();
      }
      db.closeConnectionOnly();
    }
    catch ( final Exception e ) {

    }
    return columnNames;
  }

  @Override
  public String getResourceInputNodeType() {
    return DictionaryConst.NODE_TYPE_DATA_COLUMN;
  }

  @Override
  public String getResourceOutputNodeType() {
    return null;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  @Override
  public boolean isInput() {
    return true;
  }

  @Override
  protected Set<StepField> getUsedFields( TableInputMeta meta ) {
    return null;
  }

  @Override
  public IMetaverseNode getConnectionNode() throws MetaverseAnalyzerException {
    connectionNode = (IMetaverseNode) getConnectionAnalyzer().analyze(
      getDescriptor(), baseStepMeta.getDatabaseMeta() );
    return connectionNode;
  }

  @Override
  protected void customAnalyze( TableInputMeta meta, IMetaverseNode rootNode ) throws MetaverseAnalyzerException {
    super.customAnalyze( meta, rootNode );
    rootNode.setProperty( DictionaryConst.PROPERTY_QUERY, parentTransMeta.environmentSubstitute( meta.getSQL() ) );
  }

  //////////////
  public void setBaseStepMeta( TableInputMeta meta ) {
    baseStepMeta = meta;
  }
}
