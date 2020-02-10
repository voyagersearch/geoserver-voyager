package org.geoserver.voyager;

import com.google.common.base.Throwables;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.json.HeatmapJsonFacet;
import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.common.util.NamedList;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.simple.FilteringSimpleFeatureReader;
import org.geotools.data.simple.SimpleFeatureReader;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.visitor.PostPreProcessFilterSplittingVisitor;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import static org.geoserver.voyager.VoyagerDataStore.LOG;

public class VoyagerFeatureSource extends ContentFeatureSource  {

    final VoyagerDataStore store;

    public VoyagerFeatureSource(ContentEntry entry, VoyagerDataStore store) {
        super(entry, Query.ALL);
        this.store = store;
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        VoyagerConfig config = store.config;

        Filter[] split = splitFilter(query.getFilter());
        Filter preFilter = split[0];
        Filter postFilter = split[1];

        Query preQuery = new Query(query);
        preQuery.setFilter(preFilter);

        // if no post filter we can optimize bounding box calculation
        try {
            if (postFilter == null || postFilter == Filter.INCLUDE) {
                switch (config.spatialStrategy) {
                    case RPT:
                        return boundsFromHeatMap(preQuery);
                    default:
                        throw new UnsupportedEncodingException("TODO");
                }
            } else {
                // can't optimize, need to calculate manually
                throw new UnsupportedEncodingException("TODO");
            }
        }
        catch(Exception e) {
            Throwables.propagateIfPossible(e, IOException.class);
            throw new IOException("Error calculating bounds", e);
        }
    }

    ReferencedEnvelope boundsFromHeatMap(Query query) throws Exception {
        String field = store.config.geoField;

        SolrQuery q = store.query(getSchema(), query);
        q.setFacet(true);
        q.set("facet.heatmap", field);
        q.setRows(0);

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Bounds query: " +  q.toQueryString());
        }

        QueryRequest req = new QueryRequest(q);
        QueryResponse rsp = req.process(store.solr);

        NamedList facetCounts = (NamedList) rsp.getResponse().get("facet_counts");
        NamedList facetHeatmaps = (NamedList) facetCounts.get("facet_heatmaps");

        return heatmapToBounds(new HeatmapJsonFacet((NamedList) facetHeatmaps.get(field)));
    }

    ReferencedEnvelope heatmapToBounds(HeatmapJsonFacet hm) {
        List<Coordinate> points = new ArrayList<>();

        double dy = (hm.getMaxY() - hm.getMinY()) / ((double)hm.getNumRows());
        double dx = (hm.getMaxX() - hm.getMinX() ) / ((double)hm.getNumColumns());

        for (int j = 0; j < hm.getNumRows(); j++) {
            List<Integer> grid = hm.getCountGrid().get(j);
            if (grid != null) {
                double y1 = hm.getMaxY() - j*dy;
                double y2 = hm.getMaxY() - (j+1)*dy;

                int first = 0;
                while (first < grid.size()) {
                    Integer v = grid.get(first);
                    if (v != null && v > 0) break;
                    first++;
                }

                if (first < grid.size()) {
                    int last = grid.size()-1;
                    while (last > first) {
                        Integer v = grid.get(last);
                        if (v != null && v > 0) break;
                        last--;
                    }

                    double x1 = hm.getMinX() + first*dx;
                    points.add(new Coordinate(x1, y1));
                    points.add(new Coordinate(x1, y2));

                    double x2 = hm.getMinX() + (last+1)*dx;
                    points.add(new Coordinate(x2, y1));
                    points.add(new Coordinate(x2, y2));
                }

            }
        }

        Envelope env = store.getGeometryFactory().createMultiPoint(
                CoordinateArraySequenceFactory.instance().create(points.toArray(new Coordinate[0]))
        ).getEnvelopeInternal();
        return new ReferencedEnvelope(env, getSchema().getCoordinateReferenceSystem());
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        VoyagerConfig config = store.config;

        Filter[] split = splitFilter(query.getFilter());
        Filter preFilter = split[0];
        Filter postFilter = split[1];

        Query preQuery = new Query(query);
        preQuery.setFilter(preFilter);

        // if no post filter we can optimize bounding box calculation
        try {
            if (postFilter == null || postFilter == Filter.INCLUDE) {
                return count(preQuery);
            } else {
                // can't optimize, need to calculate manually
                throw new UnsupportedEncodingException("TODO");
            }
        }
        catch(Exception e) {
            Throwables.propagateIfPossible(e, IOException.class);
            throw new IOException("Error calculating count", e);
        }
    }

    int count(Query query) throws Exception {
        SolrQuery q = store.query(getSchema(), query);
        q.setRows(0);

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Count query: " +  q.toQueryString());
        }

        QueryResponse rsp = new QueryRequest(q).process(store.solr);
        return (int) rsp.getResults().getNumFound();
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        VoyagerConfig config = store.config;

        Filter[] split = splitFilter(query.getFilter());
        Filter preFilter = split[0];
        Filter postFilter = split[1];

        Query preQuery = new Query(query);
        preQuery.setFilter(preFilter);

        SimpleFeatureReader reader;
        try {
            SolrQuery q = store.query(getSchema(), preQuery);

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Feature query: " +  q.toQueryString());
            }

            reader = new VoyagerFeatureReader(this, q);
        }
        catch(Exception e) {
            Throwables.propagateIfPossible(e, IOException.class);
            throw new IOException("Error reading features", e);
        }

        if (postFilter != null && postFilter != Filter.INCLUDE) {
            reader = new FilteringSimpleFeatureReader(reader, postFilter);
        }


        if (query.getStartIndex() != null || !query.isMaxFeaturesUnlimited()) {
            reader = new OffsetLimitSimpleFeatureReader(reader,
                    query.getStartIndex() != null && query.getStartIndex() > 0 ? query.getStartIndex() : null,
                    query.isMaxFeaturesUnlimited() ? null : query.getMaxFeatures());
        }

        return reader;
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        VoyagerConfig config = store.config;

        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        tb.setNamespaceURI(store.getNamespaceURI());
        tb.setName(config.index);
        try {
            tb.add(config.geoField, Geometry.class, CRS.decode("EPSG:4326"));
        } catch (FactoryException e) {
            throw new IOException(e);
        }
        tb.add(config.uniqueIdField, String.class);

        // use luke to get all of the other properties
        LukeRequest req = new LukeRequest();
        //req.setShowSchema(true);  // setting this doesn't return dynamic fields but means we must manually parse flags

        try {
            LukeResponse rsp = req.process(store.solr);
            for (Map.Entry<String, LukeResponse.FieldInfo> e : rsp.getFieldInfo().entrySet()) {
                String field = e.getKey();
                LukeResponse.FieldInfo info = e.getValue();
                if (field.equals(config.uniqueIdField) || field.equals(config.geoField)) continue;


                Set<FieldFlag> flags = parseFlags(info.getSchema());

                boolean storedOrDocValues = flags.contains(FieldFlag.STORED) || flags.contains(FieldFlag.DOC_VALUES);
                if (!storedOrDocValues) continue;

                VoyagerType type = VoyagerType.match(info.getType());
                tb.userData(VoyagerType.class, type);

                tb.minOccurs(0);
                tb.maxOccurs(flags.contains(FieldFlag.MULTI_VALUED) ? Integer.MAX_VALUE : 1);
                tb.add(field, type.javaClass);
            }
        }
        catch (SolrServerException e) {
            throw new IOException(e);
        }

        return tb.buildFeatureType();

    }

    private Set<FieldFlag> parseFlags(String schema) {
        Set<FieldFlag> flags = new HashSet<>();
        if (schema.contains("S")) flags.add(FieldFlag.STORED);
        if (schema.contains("D")) flags.add(FieldFlag.DOC_VALUES);
        if (schema.contains("M")) flags.add(FieldFlag.MULTI_VALUED);
        return flags;
    }

    private Filter[] splitFilter(Filter original) {
        Filter[] split = new Filter[2];
        if (original != null) {
            PostPreProcessFilterSplittingVisitor splitter =
                    new PostPreProcessFilterSplittingVisitor( store.filterCapabilities(), getSchema(), null);
            original.accept(splitter, null);
            split[0] = splitter.getFilterPre();
            split[1] = splitter.getFilterPost();
        }
        return split;
    }
}
