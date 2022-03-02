package org.geoserver.voyager;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.util.Converters;
import org.geotools.util.KVP;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class VoyagerDataStoreFactory implements DataStoreFactorySpi {
    public static final Param URL = new Param("url", URL.class, "Server URL", true,
            "https://odn.voyagersearch.com");

    public static final Param INDEX = new Param("index", String.class, "Index to Publish", true,
            "v0");

    public static final Param GEO_FIELD = new Param("geoField", String.class, "Geo Field", true,
            "geohash");

    public static final Param FILTERS = new Param("filters", String.class,
            "Comma-separated List of Filters Queries", false);

    public static final Param TIMEOUT = new Param("timeout", Integer.class,
            "Server Request Timeout", false, 10000);

    public static final Param PAGE_SIZE = new Param("pageSize", Integer.class,
            "Request Page Size", false, 100);

    public static final Param FIELD_BLACKLIST = new Param("fieldBlacklist", String.class,
            "Comma-separated List of Fields to Exclude", false);

    public static final Param NAMESPACE = new Param("namespace", URI.class, "Namespace URI", false, (Object)null, new KVP(new Object[]{"level", "advanced"}));

    static final Splitter SPLITTER = Splitter.on(Pattern.compile("\\s*,\\s*"));

    @Override
    public String getDisplayName() {
        return "Voyager Search";
    }

    @Override
    public String getDescription() {
        return "Publish the contents of a Voyager Search Solr index.";
    }

    @Override
    public Param[] getParametersInfo() {
        return new Param[]{ URL, INDEX, GEO_FIELD, FILTERS, TIMEOUT, PAGE_SIZE, FIELD_BLACKLIST, NAMESPACE };
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public DataStore createDataStore(Map<String, ?> params) throws IOException {
        VoyagerConfig config = new VoyagerConfig();
        try {
            config.uri = param(URL, params, URL.class).toURI().toString();
            config.index = param(INDEX, params, String.class);
            config.geoField = param(GEO_FIELD, params, String.class);
            config.timeout = param(TIMEOUT, params, Integer.class);
            config.pageSize = param(PAGE_SIZE, params, Integer.class);
            config.filters = Optional.ofNullable(param(FILTERS, params, String.class))
                    .map(SPLITTER::splitToList).orElse(Collections.emptyList());
            config.fieldBlacklist = Optional.ofNullable(param(FIELD_BLACKLIST, params, String.class))
                    .map(SPLITTER::splitToList).orElse(Collections.emptyList());

            VoyagerDataStore store = new VoyagerDataStore(config);
            Optional.ofNullable(NAMESPACE.lookUp(params)).map(Object::toString).ifPresent(store::setNamespaceURI);
            return store;
        }
        catch(Exception e) {
            Throwables.propagateIfPossible(e, IOException.class);
            throw new IOException(e);
        }
    }

    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        throw new UnsupportedOperationException();
    }

    <T> T param(Param p, Map<String,?> params, Class<T> type) {
        Object v = null;
        try {
            v = p.lookUp(params);
        }
        catch (IOException e) {}

        if (v == null) {
            v = p.sample;
        }

        if (p.required && v == null) throw new IllegalArgumentException("Parameter " + p.key + " not specified");

        return v != null ? Converters.convert(v, type) : null;
    }
}
