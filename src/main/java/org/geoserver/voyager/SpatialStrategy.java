package org.geoserver.voyager;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;

public enum SpatialStrategy {
    RPT;

    public String encode(Geometry geometry) {
        WKTWriter writer = new WKTWriter();
        return writer.write(geometry);
    }
}
