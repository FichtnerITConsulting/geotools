package org.geotools.data.hana;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import org.geotools.filter.FilterCapabilities;
import org.geotools.jdbc.SQLDialect;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
//import org.opengis.filter.FilterCapabilities;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.DistanceBufferOperator;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class HanaFilter2SQLHelper {

    protected static final String IO_ERROR = "io problem writing filter";
    //private static final Envelope WORLD = new Envelope(-180, 180, -90, 90);
    
    HanaFilterToSQL delegate;
    Writer out;
    boolean looseBBOXEnabled;

    public HanaFilter2SQLHelper(HanaFilterToSQL delegate) {
        this.delegate = delegate;
    }

    public static FilterCapabilities createFilterCapabilities(boolean encodeFunctions) {
        FilterCapabilities caps = new FilterCapabilities();
        caps.addAll(SQLDialect.BASE_DBMS_CAPABILITIES);

        // adding the spatial filters support
        caps.addType(BBOX.class);
        caps.addType(Contains.class);
        caps.addType(Crosses.class);
        caps.addType(Disjoint.class);
        caps.addType(Equals.class);
        caps.addType(Intersects.class);
        caps.addType(Overlaps.class);
        caps.addType(Touches.class);
        caps.addType(Within.class);
        caps.addType(DWithin.class);
        caps.addType(Beyond.class);
        
        return caps;
    }

    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
            PropertyName property, Literal geometry, boolean swapped,
            Object extraData) {
        try {
            if (filter instanceof DistanceBufferOperator) {
                visitDistanceSpatialOperator((DistanceBufferOperator) filter,
                        property, geometry, swapped, extraData);
            } else {
                visitComparisonSpatialOperator(filter, property, geometry,
                        swapped, extraData);
            }
        } catch (IOException e) {
            throw new RuntimeException(IO_ERROR, e);
        }
        return extraData;
    }
    
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1,
        Expression e2, Object extraData) {
        
        try {
            visitBinarySpatialOperator(filter, e1, e2, false, extraData);
        } catch (IOException e) {
            throw new RuntimeException(IO_ERROR, e);
        }
        return extraData;
    }
    void visitDistanceSpatialOperator(DistanceBufferOperator filter,
            PropertyName property, Literal geometry, boolean swapped,
            Object extraData) throws IOException {
        if ((filter instanceof DWithin && !swapped)
                || (filter instanceof Beyond && swapped)) {
        	property.accept(delegate, extraData);
            out.write(".ST_WithinDistance(");
            geometry.accept(delegate, extraData);
            out.write(",");
            out.write(Double.toString(filter.getDistance()));
            out.write(")");
        }
        if ((filter instanceof DWithin && swapped)
                || (filter instanceof Beyond && !swapped)) {
        	property.accept(delegate, extraData);
            out.write(".ST_Distance(");
            geometry.accept(delegate, extraData);
            out.write(") > ");
            out.write(Double.toString(filter.getDistance()));
        }
    }

    void visitComparisonSpatialOperator(BinarySpatialOperator filter,
            PropertyName property, Literal geometry, boolean swapped, Object extraData)
            throws IOException {

        // add && filter if possible
        if(!(filter instanceof Disjoint)) {
            
            property.accept(delegate, extraData);
            out.write(" && ");
            geometry.accept(delegate, extraData);
    
            // if we're just encoding a bbox in loose mode, we're done 
            if(filter instanceof BBOX && looseBBOXEnabled)
                return;
                
            out.write(" AND ");
        }

        visitBinarySpatialOperator(filter, (Expression)property, (Expression)geometry, swapped, extraData);
    }
    
    void visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, 
        boolean swapped, Object extraData) throws IOException {
        
        String closingParenthesis = ")";
        e1.accept(delegate, extraData);
          
        if (filter instanceof Equals) {
        	out.write(".ST_Equals(");
        } else if (filter instanceof Disjoint) {
            out.write(".ST_Disjoint(");
        } else if (filter instanceof Intersects || filter instanceof BBOX) {
            out.write(".ST_Intersects(");
        } else if (filter instanceof Crosses) {
            out.write(".ST_Crosses(");
        } else if (filter instanceof Within) {
            if(swapped){
                out.write(".ST_Contains(");
            }
            else{
                out.write(".ST_Within(");
            }
        } else if (filter instanceof Contains) {
            if(swapped){
                out.write(".ST_Within(");
            }
            else{
                out.write(".ST_Contains(");
            }
        } else if (filter instanceof Overlaps) {
            out.write(".ST_Overlaps(");
        } else if (filter instanceof Touches) {
            out.write(".ST_Touches(");
        } else {
            throw new RuntimeException("Unsupported filter type " + filter.getClass());
        }

        e2.accept(delegate, extraData);
        out.write(closingParenthesis);
    }

}
