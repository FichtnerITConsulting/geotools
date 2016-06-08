package org.geotools.data.hana;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.geotools.data.Parameter;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;
import org.geotools.util.KVP;
import org.geotools.util.SimpleInternationalString;

/**
 * Hana implementation of JDBCDataStoreFactory for  datastores.
 * 
 */
public class HanaDataStoreFactory extends JDBCDataStoreFactory {
	
    /** parameter for database type */
    public static final Param DBTYPE = new Param("dbtype", String.class, "Type", true, "hana");
    
    /** enables using && in bbox queries */
    public static final Param LOOSEBBOX = new Param("Loose bbox", Boolean.class, "Perform only primary filter on bbox", false, Boolean.TRUE);
    
    /** parameter that enables estimated extends instead of exact ones */ 
    public static final Param ESTIMATED_EXTENTS = new Param("Estimated extends", Boolean.class, "Use the spatial index information to quickly get an estimate of the data bounds", false, Boolean.TRUE);
    
    public static final Param HOST = new Param("host", String.class, "Host", true, "localhost");
    
    /** parameter for database port */
    public static final Param PORT = new Param("port", Integer.class, "Port", true, 30015);  
    
    /** parameter for database password */
    public static final Param PASSWD = new Param("passwd", String.class,
            new SimpleInternationalString("password used to login"), true, null, Collections
                    .singletonMap(Parameter.IS_PASSWORD, Boolean.TRUE));

    
    /** parameter for database schema */
    public static final Param SCHEMA = new Param("schema", String.class, "Schema", false, "public");
    
    /** attempt to create the database if missing */
    public static final Param CREATE_DB_IF_MISSING = new Param("create database", Boolean.class, 
            "Creates the database if it does not exist yet", false, false, Param.LEVEL, "advanced");
    
    /** attempt to create the database if missing */
    public static final Param CREATE_PARAMS = new Param("create database params", String.class, 
            "Extra specifications appeneded to the CREATE DATABASE command", false, "", Param.LEVEL, "advanced");

    /**
     * Whether a prepared statements based dialect should be used, or not
     */
    public static final Param PREPARED_STATEMENTS = new Param("preparedStatements", Boolean.class, "Use prepared statements", false, Boolean.FALSE);
    
    /**
     * Enables direct encoding of selected filter functions in sql
     */
    public static final Param ENCODE_FUNCTIONS = new Param( "encode functions", Boolean.class,
            "set to true to have a set of filter functions be translated directly in SQL. " +
            "Due to differences in the type systems the result might not be the same as evaluating " +
            "them in memory, including the SQL failing with errors while the in memory version works fine. " +
            "However this allows to push more of the filter into the database, increasing performance." +
            "the postgis table.", false, new Boolean(false),
            new KVP( Param.LEVEL, "advanced"));
    
    /**
     * Enables usage of ST_Simplify when the queries contain geometry simplification hints
     */
    public static final Param SIMPLIFY = new Param("Support on the fly geometry simplification", Boolean.class, 
            "When enabled, operations such as map rendering will pass a hint that will enable the usage of ST_Simplify", false, Boolean.TRUE);
	
    @Override
    protected String getDatabaseID() {
        return (String) DBTYPE.sample;
    }

    @Override
    protected String getDriverClassName() {
        return "com.sap.db.jdbc.Driver";
    }

    @Override
    protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
        return new HanaDialect2(dataStore);
    }

    @Override
    protected String getValidationQuery() {
        return "select current_date from dummy";// "SELECT count(SRS_OID) from SYS.ST_SPATIAL_REFERENCE_SYSTEMS_";
    }

    
    public String getDescription() {
        return "SAP HANA database";
    }
    
	@Override
	protected void setupParameters(Map parameters) {
		super.setupParameters(parameters);
        parameters.put(DBTYPE.key, DBTYPE);
        parameters.put(SCHEMA.key, SCHEMA);
        parameters.put(LOOSEBBOX.key, LOOSEBBOX);
        //parameters.put(ESTIMATED_EXTENTS.key, ESTIMATED_EXTENTS);
        parameters.put(PORT.key, PORT);
        parameters.put(PASSWD.key, PASSWD);
        //parameters.put(PREPARED_STATEMENTS.key, PREPARED_STATEMENTS);
        parameters.put(MAX_OPEN_PREPARED_STATEMENTS.key, MAX_OPEN_PREPARED_STATEMENTS);
        parameters.put(ENCODE_FUNCTIONS.key, ENCODE_FUNCTIONS);
        parameters.put(SIMPLIFY.key, SIMPLIFY);
        parameters.put(CREATE_DB_IF_MISSING.key, CREATE_DB_IF_MISSING);
        parameters.put(CREATE_PARAMS.key, CREATE_PARAMS);
        if(parameters.containsKey(DATABASE.key)){
        	parameters.remove(DATABASE.key);
        }
       
		
	}

    @Override
    protected String getJDBCUrl(Map params) throws IOException {
        String host = (String) HOST.lookUp(params);
        int port = (Integer) PORT.lookUp(params);
        return "jdbc:sap" + "://" + host + ":" + port + "/?autocommit=false";
    }

	@Override
	protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore,
			Map params) throws IOException {
		HanaDialect2 dialect = (HanaDialect2) dataStore.getSQLDialect();
        Boolean loose = (Boolean) LOOSEBBOX.lookUp(params);
        dialect.setLooseBBOXEnabled(loose == null || Boolean.TRUE.equals(loose));
        
        // check the estimated extents
        Boolean estimated = (Boolean) ESTIMATED_EXTENTS.lookUp(params);
        dialect.setEstimatedExtentsEnabled(estimated == null || Boolean.TRUE.equals(estimated));
        
        // check if we can encode functions in sql
        Boolean encodeFunctions = (Boolean) ENCODE_FUNCTIONS.lookUp(params);
        dialect.setFunctionEncodingEnabled(encodeFunctions != null && encodeFunctions);
        // check geometry simplification (on by default)
        /*
        Boolean simplify = (Boolean) SIMPLIFY.lookUp(params);
        dialect.setSimplifyEnabled(simplify == null || simplify);
		*/
		return super.createDataStoreInternal(dataStore, params);
	}
    
}
