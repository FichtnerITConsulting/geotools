package org.geotools.data.hana;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.geotools.data.DataSourceException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ByteArrayInStream;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;



/**
 * An attribute IO implementation that can manage the WKB
 *
 *
 * 
 * 
 */
public class WKBAttributeIO {
    WKBReader wkbr;
    ByteArrayInStream inStream = new ByteArrayInStream(new byte[0]);
    GeometryFactory gf;

    public WKBAttributeIO() {
        this(new GeometryFactory());
    }
    
    public WKBAttributeIO(GeometryFactory gf) {
        wkbr = new WKBReader(gf);
    }
    
    public void setGeometryFactory(GeometryFactory gf) {
        wkbr = new WKBReader(gf);
    }

    /**
     * This method will convert a WKB representation to a
     * JTS  Geometry object.
     *
     */
    private Geometry wkb2Geometry(byte[] wkbBytes)
        throws IOException {
        if (wkbBytes == null)  
            return null;
        try {
            inStream.setBytes(wkbBytes);
            if(wkbBytes.length > 15000){
            	//System.out.println(wkbBytes.length);
            }
            return wkbr.read(inStream);
        } catch (Exception e) {
            throw new DataSourceException("An exception occurred while parsing WKB data", e);
        }
    }

    /**
     * @see org.geotools.data.jdbc.attributeio.AttributeIO#read(java.sql.ResultSet,
     *      int)
     */
    public Object read(ResultSet rs, String columnName) throws IOException {
        try {
            byte bytes[] = rs.getBytes(columnName);
            if (bytes == null){ 
                return null;
            }
            //return wkb2Geometry(Base64.decode(bytes));
            return wkb2Geometry(bytes);
        } catch (SQLException e) {
            throw new DataSourceException("SQL exception occurred while reading the geometry.", e);
        }
    }
    
    /**
     * @see org.geotools.data.jdbc.attributeio.AttributeIO#read(java.sql.ResultSet,
     *      int)
     */
    public Object read(ResultSet rs, int columnIndex) throws IOException {
        try {
            byte bytes[] = rs.getBytes(columnIndex);
            if (bytes == null){ // ie. its a null column -> return a null geometry!
                return null;
            }
            //return wkb2Geometry(Base64.decode(bytes));
            return wkb2Geometry(bytes);
        } catch (SQLException e) {
            throw new DataSourceException("SQL exception occurred while reading the geometry.", e);
        }
    }

    /**
     * @see org.geotools.data.jdbc.attributeio.AttributeIO#write(java.sql.PreparedStatement, int, java.lang.Object)
     */
    public void write(PreparedStatement ps, int position, Object value) throws IOException {
        try {
            if(value == null) {
                ps.setNull(position, Types.OTHER);
            } else {
                ps.setBytes( position, new WKBWriter().write( (Geometry)value ));
            }
        } catch (SQLException e) {
            throw new DataSourceException("SQL exception occurred while reading the geometry.", e);
        }

    }
    
    /**
     * Turns a char that encodes four bits in hexadecimal notation into a byte
     *
     * @param c
     *
     */
    public static byte getFromChar(char c) {
        if (c <= '9') {
            return (byte) (c - '0');
        } else if (c <= 'F') {
            return (byte) (c - 'A' + 10);
        } else {
            return (byte) (c - 'a' + 10);
        }
    }
}

