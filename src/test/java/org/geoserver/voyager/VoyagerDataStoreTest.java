package org.geoserver.voyager;

import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.util.logging.Logging;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VoyagerDataStoreTest {

    @BeforeClass
    public static void logging() {
        ConsoleHandler h = new ConsoleHandler();
        h.setLevel(Level.FINE);

        Logger l = Logging.getLogger("");
        l.addHandler(h);
        l.setLevel(Level.FINE);
    }

    VoyagerDataStore store;

    @Before
    public void setup() {
        VoyagerConfig config = VoyagerConfig.local();
        config.filters.add("geohash:*");
        store = new VoyagerDataStore(config);
    }

    @After
    public void destroy() {
        store.dispose();
    }

    @Test
    public void bounds() throws Exception {
        SimpleFeatureSource source = store.getFeatureSource("v0");
        assertNotNull(source.getBounds());
    }

    @Test
    public void count() throws Exception {
        SimpleFeatureSource source = store.getFeatureSource("v0");
        assertTrue(source.getCount(Query.ALL) > 0);
    }

    @Test
    public void features() throws Exception {
        SimpleFeatureSource source = store.getFeatureSource("v0");
        SimpleFeatureCollection features = source.getFeatures();

        SimpleFeatureIterator it = features.features();
        assertTrue(it.hasNext());
        while (it.hasNext()) {
            SimpleFeature f = it.next();
            assertNotNull(f);
            assertNotNull(f.getDefaultGeometry());
        }
    }

    @Test
    public void filterByBOX() throws Exception {
        Envelope e = new WKTReader().read("POLYGON ((-79.04654 40.97563, -79.04654 41.52007, -77.82233 41.52007, -77.82233 40.97563, -79.04654 40.97563))").getEnvelopeInternal();
        FilterFactory ff = CommonFactoryFinder.getFilterFactory();
        Filter bbox = ff.bbox("geohash", e.getMinX(), e.getMinY(), e.getMaxX(), e.getMaxY(), "epsg:4326");

        SimpleFeatureSource source = store.getFeatureSource("v0");
        SimpleFeatureIterator it = source.getFeatures(bbox).features();
        assertTrue(it.hasNext());
        while (it.hasNext()) {
            SimpleFeature f = it.next();
            assertNotNull(f);
            System.out.println(f);
        }
    }

    @Test
    public void schema() throws Exception {
        SimpleFeatureSource source = store.getFeatureSource("v0");
        source.getSchema().getAttributeDescriptors().forEach(d -> {
            System.out.println(d.getLocalName());
        });
    }
}
