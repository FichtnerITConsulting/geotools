/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2008, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.data.hana;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;

import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.factory.Hints;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.ColumnMetadata;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.VirtualTable;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * 
 * This class used by Hana JDBCDataStore to directly communicate with the database.
 * It encapsulates all the database specific operations that JDBCDataStore
 * needs to function.
 * 
 */
public class HanaDialect2 extends BasicSQLDialect {

	//geometry type to class map
    final static Map<String, Class> TYPE_TO_CLASS_MAP = new HashMap<String, Class>() {
        {
            put("ST_GEOMETRY", Geometry.class);
            put("ST_POINT", Point.class);
            put("ST_LINESTRING", LineString.class);
            put("ST_POLYGON", Polygon.class);
            put("ST_MULTIPOINT", MultiPoint.class);
            put("ST_MULTILINESTRING", MultiLineString.class);
            put("ST_MULTIPOLYGON", MultiPolygon.class);
        }
    };
     static int tempInsertVT =0;
    //geometry class to type map
    final static Map<Class, String> CLASS_TO_TYPE_MAP = new HashMap<Class, String>() {
        {
            put(Geometry.class, "ST_GEOMETRY");
            put(Point.class, "ST_POINT");
            put(LineString.class, "ST_LINESTRING");
            put(Polygon.class, "ST_POLYGON");
            put(MultiPoint.class, "ST_MULTIPOINT");
            put(MultiLineString.class, "ST_MULTILINESTRING");
            put(MultiPolygon.class, "ST_MULTIPOLYGON");
        }
    };
    
    
    /*
    @Override
    public boolean isAggregatedSortSupported(String function) {
       return "distinct".equalsIgnoreCase(function);
    }
*/
    public HanaDialect2(JDBCDataStore dataStore) {
        super(dataStore);
    }

    boolean looseBBOXEnabled = false;

    boolean estimatedExtentsEnabled = false;
    
    boolean functionEncodingEnabled = false;
    
    boolean simplifyEnabled = false;
    
    String lastInitializedDatabaseSchema=null;
    
    public boolean isLooseBBOXEnabled() {
        return looseBBOXEnabled;
    }

    public void setLooseBBOXEnabled(boolean looseBBOXEnabled) {
        this.looseBBOXEnabled = looseBBOXEnabled;
    }
        
    public boolean isEstimatedExtentsEnabled() {
        return estimatedExtentsEnabled;
    }

    public void setEstimatedExtentsEnabled(boolean estimatedExtentsEnabled) {
        this.estimatedExtentsEnabled = estimatedExtentsEnabled;
    }
    
    public boolean isFunctionEncodingEnabled() {
        return functionEncodingEnabled;
    }

    public void setFunctionEncodingEnabled(boolean functionEncodingEnabled) {
        this.functionEncodingEnabled = functionEncodingEnabled;
    }
    
    public boolean isSimplifyEnabled() {
        return simplifyEnabled;
    }

    /**
     * 
     * @param simplifyEnabled
     */
    public void setSimplifyEnabled(boolean simplifyEnabled) {
        this.simplifyEnabled = simplifyEnabled;
    }


    @Override
    public void initializeConnection(Connection cx) throws SQLException {
        String schema = dataStore.getDatabaseSchema();
        if(lastInitializedDatabaseSchema == null || !lastInitializedDatabaseSchema.equalsIgnoreCase(schema)){
        	lastInitializedDatabaseSchema = schema;
	        initVirtualTables(schema, cx);
	        super.initializeConnection(cx);
        }
    }

    @Override
    public boolean includeTable(String schemaName, String tableName,
            Connection cx) throws SQLException {
    	return !tableName.equals("ST_GEOMETRY_COLUMNS");
    }

    ThreadLocal<WKBAttributeIO> wkbReader = new ThreadLocal<WKBAttributeIO>();

    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#decodeGeometryValue(org.opengis.feature.type.GeometryDescriptor, java.sql.ResultSet, java.lang.String, com.vividsolutions.jts.geom.GeometryFactory, java.sql.Connection)
     */
    @Override
    public Geometry decodeGeometryValue(GeometryDescriptor descriptor,
            ResultSet rs, String column, GeometryFactory factory, Connection cx)
            throws IOException, SQLException {
        WKBAttributeIO reader = getWKBReader(factory);
        
        return (Geometry) reader.read(rs, column);
    }
    
    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#decodeGeometryValue(org.opengis.feature.type.GeometryDescriptor, java.sql.ResultSet, int, com.vividsolutions.jts.geom.GeometryFactory, java.sql.Connection)
     */
    @Override
    public Geometry decodeGeometryValue(GeometryDescriptor descriptor,
            ResultSet rs, int column, GeometryFactory factory, Connection cx)
            throws IOException, SQLException {
        WKBAttributeIO reader = getWKBReader(factory);
        
        try {
			return (Geometry) reader.read(rs, column);
		} 
        catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
        catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return (Geometry) reader.read(rs, column);
    }

    private WKBAttributeIO getWKBReader(GeometryFactory factory) {
        WKBAttributeIO reader = wkbReader.get();
        if(reader == null) {
            reader = new WKBAttributeIO(factory);
            wkbReader.set(reader);
        }  else {
            reader.setGeometryFactory(factory);
        }
        return reader;
    }

    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#encodeGeometryColumn(org.opengis.feature.type.GeometryDescriptor, java.lang.String, int, java.lang.StringBuffer)
     */
    @Override
    public void encodeGeometryColumn(GeometryDescriptor gatt, String prefix, int srid,
            StringBuffer sql) {
        encodeGeometryColumn(gatt, prefix, srid, null, sql);
    }

    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#encodeGeometryColumn(org.opengis.feature.type.GeometryDescriptor, java.lang.String, int, org.geotools.factory.Hints, java.lang.StringBuffer)
     */
    @Override
    public void encodeGeometryColumn(GeometryDescriptor gatt, String prefix, int srid, Hints hints, 
        StringBuffer sql) {
    
        boolean force2D = hints != null && hints.containsKey(Hints.FEATURE_2D) && 
            Boolean.TRUE.equals(hints.get(Hints.FEATURE_2D));
        
        //super.encodeGeometryColumn(gatt, prefix, srid, hints, sql);
        

        if (force2D) {
            encodeColumnName(prefix, gatt.getLocalName(), sql);
            sql.append(".ST_AsWKB()");
        } else {
            encodeColumnName(prefix, gatt.getLocalName(), sql);
            sql.append(".ST_AsEWKB()");
        }
        int a=1;
        
    }
    
    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#encodeGeometryColumnSimplified(org.opengis.feature.type.GeometryDescriptor, java.lang.String, int, java.lang.StringBuffer, java.lang.Double)
     
    @Override
    public void encodeGeometryColumnSimplified(GeometryDescriptor gatt, String prefix, int srid,
            StringBuffer sql, Double distance) {
    	
            super.encodeGeometryColumnSimplified(gatt, prefix, srid, sql, distance);
    }
*/
    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#encodeGeometryEnvelope(java.lang.String, java.lang.String, java.lang.StringBuffer)
     */
    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn,
            StringBuffer sql) {
    	sql.append(geometryColumn+".st_envelope().st_astext()");
    }
    
    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#getOptimizedBounds(java.lang.String, org.opengis.feature.simple.SimpleFeatureType, java.sql.Connection)
     */
    @Override
    public List<ReferencedEnvelope> getOptimizedBounds(String schema, SimpleFeatureType featureType,
            Connection cx) throws SQLException, IOException {
        if (!estimatedExtentsEnabled){
            return null;
        }
        return null;
        /*
        String tableName = featureType.getTypeName();

        Statement st = null;
        ResultSet rs = null;

        List<ReferencedEnvelope> result = new ArrayList<ReferencedEnvelope>();
        Savepoint savePoint = null;
        try {
            st = cx.createStatement();
            if(!cx.getAutoCommit()) {
                savePoint = cx.setSavepoint();
            }

            for (AttributeDescriptor att : featureType.getAttributeDescriptors()) {
                if (att instanceof GeometryDescriptor) {
                    // use estimated extent (optimizer statistics)
                    StringBuffer sql = new StringBuffer();
                    sql.append("select ST_AsText(ST_force_2d(ST_Envelope(ST_Estimated_Extent('");
                    if(schema != null) {
                        sql.append(schema);
                        sql.append("', '");
                    }
                    sql.append(tableName);
                    sql.append("', '");
                    sql.append(att.getName().getLocalPart());
                    sql.append("'))))");
                    rs = st.executeQuery(sql.toString());

                    if (rs.next()) {
                        // decode the geometry
                        Envelope env = decodeGeometryEnvelope(rs, 1, cx);

                        // reproject and merge
                        if (!env.isNull()) {
                            CoordinateReferenceSystem crs = ((GeometryDescriptor) att)
                                    .getCoordinateReferenceSystem();
                            result.add(new ReferencedEnvelope(env, crs));
                        }
                    }
                    rs.close();
                }
            }
        } catch(SQLException e) {
            if(savePoint != null) {
                cx.rollback(savePoint);
            }
            LOGGER.log(Level.WARNING, "Failed to use ST_Estimated_Extent, falling back on envelope aggregation", e);
            return null;
        } finally {
            if(savePoint != null) {
                cx.releaseSavepoint(savePoint);
            }
            dataStore.closeSafe(rs);
            dataStore.closeSafe(st);
        } 
        return result;
        */
    }

    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#decodeGeometryEnvelope(java.sql.ResultSet, int, java.sql.Connection)
     */
    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column,
            Connection cx) throws SQLException, IOException {
        try {
            String envelope = rs.getString(column);
            if (envelope != null){
                return new WKTReader().read(envelope).getEnvelopeInternal();
            }
            else {
                // empty one
                return new Envelope();
            }
        } catch (ParseException e) {
            throw (IOException) new IOException(
                    "Error occurred parsing the bounds WKT").initCause(e);
        }
    }

    /**
     * Determines the class mapping for a particular column of a table.
     * <p>
     * Implementing this method is optional. It is used to allow database to
     * perform custom type mappings based on various column metadata. It is called
     * before the mappings registered in {@link #registerSqlTypeToClassMappings(Map)}
     * and {@link #registerSqlTypeNameToClassMappings(Map) are used to determine
     * the mapping. Subclasses should implement as needed, this default implementation
     * returns <code>null</code>.
     * </p>
     * <p>
     * The <tt>columnMetaData</tt> argument is provided from
     * {@link DatabaseMetaData#getColumns(String, String, String, String)}.
     * </p>
     * @param columnMetaData The column metadata
     * @param The connection used to retrieve the metadata
     * @return The class mapped to the to column, or <code>null</code>.
     */

    @Override
    public Class<?> getMapping(ResultSet columnMetaData, Connection cx)
            throws SQLException {
        
        String typeName = columnMetaData.getString("TYPE_NAME");
        if ("uuid".equalsIgnoreCase(typeName)) {
            return UUID.class;
        }

        String gType = null;
        if ("ST_GEOMETRY".equalsIgnoreCase(typeName)) {
            gType = lookupGeometryType(columnMetaData, cx, "ST_GEOMETRY_COLUMNS", "COLUMN_NAME");
        } else {
            return null;
        }

        // decode the type into
        if (gType == null) {
            // it's either a generic geography or geometry not registered in the medatata tables
            return Geometry.class;
        } else {
            Class geometryClass = (Class) TYPE_TO_CLASS_MAP.get(gType.toUpperCase());
            if (geometryClass == null) {
                geometryClass = Geometry.class;
            }

            return geometryClass;
        }
    }

    /**
     * 
     * @param columnMetaData
     * @param cx
     * @param gTableName
     * @param gColumnName
     * @return
     * @throws SQLException
     */
    String lookupGeometryType(ResultSet columnMetaData, Connection cx, String gTableName,
            String gColumnName) throws SQLException {

		// grab the information we need to proceed
		String tableName = columnMetaData.getString("TABLE_NAME");
		String columnName = columnMetaData.getString("COLUMN_NAME");
		String schemaName = columnMetaData.getString("TABLE_SCHEM");
		
		// first attempt, try with the geometry metadata
		Connection conn = null;
		Statement statement = null;
		ResultSet result = null;
		
		try {
			String sqlStatement = "SELECT DATA_TYPE_NAME FROM " + gTableName + " WHERE " //
			  + "SCHEMA_NAME = '" + schemaName + "' " //
			  + "AND TABLE_NAME = '" + tableName + "' " //
			  + "AND " + gColumnName + " = '" + columnName + "'";
			
			LOGGER.log(Level.FINE, "Geometry type check; {0} ", sqlStatement);
			statement = cx.createStatement();
			result = statement.executeQuery(sqlStatement);
			
			if (result.next()) {
				return result.getString(1);
			}
		} finally {
			dataStore.closeSafe(result);
			dataStore.closeSafe(statement);
		}
		
		return null;
}
 
    @Override
    public void handleUserDefinedType(ResultSet columnMetaData, ColumnMetadata metadata,
            Connection cx) throws SQLException {
    	super.handleUserDefinedType(columnMetaData, metadata, cx);
    	
    }
    
    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#getGeometrySRID(java.lang.String, java.lang.String, java.lang.String, java.sql.Connection)
     */
    @Override
    public Integer getGeometrySRID(String schemaName, String tableName,
            String columnName, Connection cx) throws SQLException {
        Statement statement = null;
        ResultSet result = null;
        Integer srid = null;
        if (schemaName == null){
            schemaName = "public";
        }
        // try geometry_columns
        try {
            String sqlStatement = "SELECT SRS_ID FROM ST_GEOMETRY_COLUMNS WHERE " //
                    + "SCHEMA_NAME = '" + schemaName + "' " //
                    + "AND TABLE_NAME = '" + tableName + "' " //
                    + "AND COLUMN_NAME = '" + columnName + "'";

            LOGGER.log(Level.FINE, "Geometry srid check; {0} ", sqlStatement);
            statement = cx.createStatement();
            result = statement.executeQuery(sqlStatement);
            	int sridConst = 1000000000;
            if (result.next()) {
                srid = result.getInt(1);
                if(srid >= sridConst){
                	srid = srid%sridConst;
                }
            }
        } 
        catch(SQLException e) {
            LOGGER.log(Level.WARNING, "Failed to retrieve information about " 
                    + schemaName + "." + tableName + "."  + columnName 
                    + " from the geometry_columns table, checking the first geometry instead", e);
        } 
        finally {
            dataStore.closeSafe(result);
        }
        if(srid == null) {
        	srid = 4326;
        }

        return srid;
    }
    
    @Override
    public int getGeometryDimension(String schemaName, String tableName, String columnName,
            Connection cx) throws SQLException {
    	return super.getGeometryDimension(schemaName, tableName, columnName, cx);
    }

    @Override
    public String getSequenceForColumn(String schemaName, String tableName,
            String columnName, Connection cx) throws SQLException {
        return "SEQ1";
    }

    @Override
    public Object getNextSequenceValue(String schemaName, String sequenceName,
            Connection cx) throws SQLException {
        Statement st = cx.createStatement();
        try {
            String sql = "SELECT \""+ sequenceName + "\".NEXTVAL FROM DUMMY";

            dataStore.getLogger().fine(sql);
            ResultSet rs = st.executeQuery(sql);
            try {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            } finally {
                dataStore.closeSafe(rs);
            }
        } finally {
            dataStore.closeSafe(st);
        }

        return null;
    }

    @Override
    public boolean lookupGeneratedValuesPostInsert() {
        return true;
    }
    
    @Override
    public Object getLastAutoGeneratedValue(String schemaName, String tableName, String columnName,
            Connection cx) throws SQLException {
        /*
        Statement st = cx.createStatement();
        try {
            String sql = "SELECT lastval()";
            dataStore.getLogger().fine( sql);
            
            ResultSet rs = st.executeQuery( sql);
            try {
                if ( rs.next() ) {
                    return rs.getLong(1);
                }
            } 
            finally {
                dataStore.closeSafe(rs);
            }
        }
        finally {
            dataStore.closeSafe(st);
        }
*/
        return null;
    }
    
    @Override
    public void registerClassToSqlMappings(Map<Class<?>, Integer> mappings) {
        super.registerClassToSqlMappings(mappings);

        mappings.put(Geometry.class, Types.OTHER);
        mappings.put(UUID.class, Types.OTHER);
    }

    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#registerSqlTypeNameToClassMappings(java.util.Map)
     */
    @Override
    public void registerSqlTypeNameToClassMappings(
            Map<String, Class<?>> mappings) {
        super.registerSqlTypeNameToClassMappings(mappings);

        mappings.put("ST_GEOMETRY", Geometry.class);
        mappings.put("ST_POINT", Point.class);
        mappings.put("POINT", Point.class);
        mappings.put("TEXT", String.class);
        mappings.put("BIGINT", Long.class);
        mappings.put("INTEGER", Integer.class);
        mappings.put("BOOLEAN", Boolean.class);
        mappings.put("VARCHAR", String.class);
        mappings.put("DOUBLE", Double.class);
        mappings.put("REAL", Float.class);
        mappings.put("SMALLINT", Short.class);
        mappings.put("TIME", Time.class);
        mappings.put("TIMESTAMP", Timestamp.class);
        mappings.put("DECIMAL", Float.class);
    }
    
    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#registerSqlTypeToSqlTypeNameOverrides(java.util.Map)
     */
    @Override
    public void registerSqlTypeToSqlTypeNameOverrides(
            Map<Integer, String> overrides) {
        overrides.put(Types.VARCHAR, "VARCHAR");
        overrides.put(Types.BOOLEAN, "BOOL");
    }

    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.SQLDialect#getGeometryTypeName(java.lang.Integer)
     */
    @Override
    public String getGeometryTypeName(Integer type) {
        return "ST_GEOMETRY";
    }

    @Override
    public void encodePrimaryKey(String column, StringBuffer sql) {
        encodeColumnName(column, sql);
        sql.append(" SERIAL PRIMARY KEY");
    }

    /**
     * Creates GEOMETRY_COLUMN registrations
     */
    @Override
    public void postCreateTable(String schemaName,
            SimpleFeatureType featureType, Connection cx) throws SQLException {
        schemaName = schemaName != null ? schemaName : "public";
        String tableName = featureType.getName().getLocalPart();

        Statement st = null;
        try {
            st = cx.createStatement();

            // register all geometry columns in the database
            for (AttributeDescriptor att : featureType
                    .getAttributeDescriptors()) {
                if (att instanceof GeometryDescriptor) {
                    GeometryDescriptor gd = (GeometryDescriptor) att;

                    // lookup or reverse engineer the srid
                    int srid = -1;
                    if (gd.getUserData().get(JDBCDataStore.JDBC_NATIVE_SRID) != null) {
                        srid = (Integer) gd.getUserData().get(
                                JDBCDataStore.JDBC_NATIVE_SRID);
                    } else if (gd.getCoordinateReferenceSystem() != null) {
                        try {
                            Integer result = CRS.lookupEpsgCode(gd
                                    .getCoordinateReferenceSystem(), true);
                            if (result != null)
                                srid = result;
                        } catch (Exception e) {
                            LOGGER.log(Level.FINE, "Error looking up the "
                                    + "epsg code for metadata "
                                    + "insertion, assuming -1", e);
                        }
                    }

                    // grab the geometry type
                    String geomType = CLASS_TO_TYPE_MAP.get(gd.getType()
                            .getBinding());
                    if (geomType == null)
                        geomType = "ST_GEOMETRY";

                    String sql = null;
                    // register the geometry type, first remove and eventual
                    // leftover, then write out the real one
                    sql =
                            "DELETE FROM ST_GEOMETRY_COLUMNS"
                                    + " WHERE SCHEMA_NAME = '" + schemaName + "'" //
                                    + " AND TABLE_NAME = '" + tableName + "'" //
                                    + " AND COLUMN_NAME = '" + gd.getLocalName() + "'";

                    LOGGER.fine(sql);
                    st.execute(sql);
/*
                    // add the spatial index
                    sql =
                            "CREATE INDEX \"spatial_" + tableName //
                                    + "_" + gd.getLocalName().toLowerCase() + "\""//
                                    + " ON " //
                                    + "\"" + schemaName + "\"" //
                                    + "." //
                                    + "\"" + tableName + "\"" //
                                    + " USING GIST (" //
                                    + "\"" + gd.getLocalName() + "\"" //
                                    + ")";
                    LOGGER.fine(sql);
                    st.execute(sql);
*/                    
                }
            }
            if (!cx.getAutoCommit()) {
                cx.commit();
            }
        } finally {
            dataStore.closeSafe(st);
        }
    }
    
    @Override
    public void postDropTable(String schemaName, SimpleFeatureType featureType, Connection cx)
            throws SQLException {
        Statement st = cx.createStatement();
        String tableName = featureType.getTypeName();

        try {
            //remove all the geometry_column entries
            String sql = 
                "DELETE FROM GEOMETRY_COLUMNS"
                    + " WHERE f_table_catalog=''" //
                    + " AND f_table_schema = '" + schemaName + "'" 
                    + " AND f_table_name = '" + tableName + "'";
            LOGGER.fine( sql );
            st.execute( sql );
        }
        finally {
            dataStore.closeSafe(st);
        }
    }

    @Override
    public void encodeGeometryValue(Geometry value, int dimension, int srid, StringBuffer sql)
            throws IOException {
        if (value == null || value.isEmpty()) {
            sql.append("NULL");
        } else {
            if (value instanceof LinearRing) {
                value = value.getFactory().createLineString(((LinearRing) value).getCoordinateSequence());
                sql.append("new ST_MultiPolygon('" + value.toText() + "')");
            }
            else if(value instanceof Point){
                value = value.getFactory().createPoint(((Point)value).getCoordinateSequence());
                sql.append("new ST_Point('" + value.toText() + "')");
            }
            else if(value instanceof LineString){
                value = value.getFactory().createPoint(((LineString)value).getCoordinateSequence());
                sql.append("new ST_LineString('" + value.toText() + "')");
            }
            else {
            	sql.append("NULL");
            }
        }
    }

    @Override
    public FilterToSQL createFilterToSQL() {
        HanaFilterToSQL sql = new HanaFilterToSQL(this);
        sql.setLooseBBOXEnabled(looseBBOXEnabled);
       // sql.setFunctionEncodingEnabled(functionEncodingEnabled);
    	//FilterToSQL sql = super.createFilterToSQL(); 
    	//sql.setLooseBBOXEnabled(looseBBOXEnabled);
        return sql;
    }
    
    @Override
    public boolean isLimitOffsetSupported() {
        return true;
    }
    
    @Override
    public void applyLimitOffset(StringBuffer sql, int limit, int offset) {
        if(limit >= 0 && limit < Integer.MAX_VALUE) {
            sql.append(" LIMIT " + limit);
            if(offset > 0) {
                sql.append(" OFFSET " + offset);
            }
        } else if(offset > 0) {
            sql.append(" OFFSET " + offset);
        }
    }
    
    /*
     * (non-Javadoc)
     * @see org.geotools.jdbc.BasicSQLDialect#encodeValue(java.lang.Object, java.lang.Class, java.lang.StringBuffer)
     */
    @Override
    public void encodeValue(Object value, Class type, StringBuffer sql) {
        if(byte[].class.equals(type)) {
            byte[] input = (byte[]) value;
                encodeByteArrayAsHex(input, sql);
        } 
        else {
            super.encodeValue(value, type, sql);
        }
    }

    void encodeByteArrayAsHex(byte[] input, StringBuffer sql) {
        StringBuffer sb = new StringBuffer("\\x");
        for (int i = 0; i < input.length; i++) {
            sb.append(String.format("%02x", input[i]));
        }
        super.encodeValue(sb.toString(), String.class, sql);
    }

    @Override
    public int getDefaultVarcharSize(){
        return -1;
    }

    
    protected void addSupportedHints(Set<Hints.Key> hints) {    
        if(isSimplifyEnabled()) {
            hints.add(Hints.GEOMETRY_SIMPLIFICATION);
        }
    }
    
    /**
     * This method create virtual tables from hana specific view types.
     * @param schemaName
     * @param cx
     */
    public void initVirtualTables(String schemaName, Connection cx)  {
    	try {
            DatabaseMetaData metaData = cx.getMetaData();
            Set<String> availableTableTypes = new HashSet<String>();
    		
			if(true){
				
				//Hana specific view types
		        String[] desiredTableTypes = new String[] {
		               "CALC VIEW", "HIERARCHY VIEW", "OLAP VIEW", "JOIN VIEW"
		            };
		            ResultSet tableTypes = null;
		            tableTypes = metaData.getTableTypes();
		            while(tableTypes.next()){
		                availableTableTypes.add(tableTypes.getString("TABLE_TYPE"));
		            }
		            dataStore.closeSafe(tableTypes);
		            
		            Set<String> queryTypes = new HashSet<String>();
		            for (String desiredTableType : desiredTableTypes) {
		                if(availableTableTypes.contains(desiredTableType)){
		                    queryTypes.add(desiredTableType);
		                }
		            }
		            ResultSet tables = metaData.getTables(null, schemaName, "%",
		                    queryTypes.toArray(new String[0]));
		            tables.setFetchSize(500);
				    while(tables.next()){
	                    String tableName = tables.getString("TABLE_NAME");
	                    List<ColumnMetadata> cmList = this.getGeometryColumnMetadata(cx, schemaName, tableName);
	                    if(!cmList.isEmpty()){
							StringBuffer sb = new StringBuffer();
						    sb.append("select * from ");
						    this.encodeTableName(schemaName, sb);
						    sb.append(".");
						    this.encodeTableName(tableName, sb);
						    VirtualTable vt = new VirtualTable(tableName, sb.toString());
		                    for (ColumnMetadata cm : cmList) {
								    vt.addGeometryMetadatata(cm.getName(), cm.getBinding(), 4326);
							}
		                    dataStore.createVirtualTable(vt);
	                    }
				    }
				 dataStore.closeSafe(tables);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	finally{
    		
    	}
    }
    
    List<ColumnMetadata> getGeometryColumnMetadata(Connection cx, String databaseSchema, String tableName)
            throws SQLException {
    	//ColumnMetadata column = null;
    	List<ColumnMetadata> geomColumnList = new ArrayList<ColumnMetadata>();
        DatabaseMetaData metaData = cx.getMetaData();

        ResultSet columns = metaData.getColumns(cx.getCatalog(), databaseSchema, tableName, "%");
        if(dataStore.getFetchSize() > 0) {
            columns.setFetchSize(dataStore.getFetchSize());
        }

        try {
        	
            while (columns.next()) {
            	Class geometryClass = (Class) TYPE_TO_CLASS_MAP.get(columns.getString("TYPE_NAME").toUpperCase());
            	//Only geometry column 
            	if(geometryClass != null){
            		ColumnMetadata column = new ColumnMetadata();
	                column.setName(columns.getString("COLUMN_NAME"));
	                column.setBinding(geometryClass);
	                column.setTypeName(columns.getString("TYPE_NAME"));
	                column.setSqlType(columns.getInt("DATA_TYPE"));
	                geomColumnList.add(column);
            	}
            }
        } catch(SQLException ex){
        	ex.printStackTrace();
        }
        finally {    
        	dataStore.closeSafe(columns);
        }

        return geomColumnList;
    }
    
}
