package org.geoserver.voyager;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class VoyagerConfig {
    public String uri = "http://localhost:8888";
    public String uniqueIdField = "id";
    public String index = "v0";
    public SpatialStrategy spatialStrategy = SpatialStrategy.RPT;
    public String geoField = "geohash";
    public List<String> filters = new ArrayList<>();
    public List<String> fieldBlacklist = new ArrayList<>();
    public int timeout = 10000;
    public int pageSize = 100;

    public String solrUri() {
        return StringUtils.join(new String[]{uri, "solr", index}, '/');
    }

    public boolean includesField(String field) {
        return !fieldBlacklist.contains(field);
    }

    public static VoyagerConfig local() {
        return new VoyagerConfig();
    }

    public static VoyagerConfig odn() {
        VoyagerConfig config = new VoyagerConfig();
        config.uri = "https://odn.voyagersearch.com";
        config.timeout = 20000;
        return config;
    }
}

