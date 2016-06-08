/*
 * 
 */
package org.geotools.data.hana;

import java.io.IOException;

import org.geotools.data.jdbc.FilterToSQL;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BinarySpatialOperator;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LinearRing;

/**
 *
 *
 * 
 */
public class HanaFilterToSQL extends FilterToSQL {

    HanaFilter2SQLHelper helper;
    
	public HanaFilterToSQL(HanaDialect2 dialect) {
		this.helper = new HanaFilter2SQLHelper(this);
	}

    public boolean isLooseBBOXEnabled() {
        return helper.looseBBOXEnabled;
    }

    public void setLooseBBOXEnabled(boolean looseBBOXEnabled) {
        helper.looseBBOXEnabled = looseBBOXEnabled;
    }

    @Override
    protected void visitLiteralGeometry(Literal expression) throws IOException {
        // evaluate the literal and store it for later
        Geometry geom  = (Geometry) evaluateLiteral(expression, Geometry.class);
        
        if ( geom instanceof LinearRing ) {
            geom = geom.getFactory().createLineString(((LinearRing) geom).getCoordinateSequence());
        }
        
        //Object typename = currentGeometry.getUserData().get(JDBCDataStore.JDBC_NATIVE_TYPENAME);

        out.write("ST_Geometry('");
        out.write(geom.toText());
        out.write(")");
        
    }

    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
            PropertyName property, Literal geometry, boolean swapped,
            Object extraData) {
        helper.out = out;
        return helper.visitBinarySpatialOperator(filter, property, geometry,
                swapped, extraData);
    }
    
    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, 
            Expression e2, Object extraData) {
        helper.out = out;
        return helper.visitBinarySpatialOperator(filter, e1, e2, extraData);
    }

    GeometryDescriptor getCurrentGeometry() {
        return currentGeometry;
    }
}
