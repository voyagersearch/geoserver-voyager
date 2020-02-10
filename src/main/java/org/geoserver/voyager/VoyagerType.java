package org.geoserver.voyager;

import org.locationtech.jts.geom.Envelope;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum VoyagerType {
    DATE(Date.class),
    MS(Long.class),
    DOUBLE(Double.class),
    FLOAT(Float.class),
    INT(Integer.class),
    LONG(Long.class),
    BOOLEAN(Boolean.class),
    DVFLAG(Boolean.class),
    FLAG(Boolean.class),
    BYTES(Integer.class),
    BBOX(Envelope.class),
    STRING(String.class);

    final Class<?> javaClass;
    VoyagerType(Class<?> javaClass) {
        this.javaClass = javaClass;
    }

    static Map<String,VoyagerType> TYPES = new HashMap<>();
    static {
        for (VoyagerType t : values()) {
            TYPES.put(t.name(), t);
            TYPES.put(t.name().toLowerCase(), t);
        }
    }

    public static VoyagerType match(String name) {
        return Optional.ofNullable(TYPES.get(name)).orElse(STRING);
    }
}
