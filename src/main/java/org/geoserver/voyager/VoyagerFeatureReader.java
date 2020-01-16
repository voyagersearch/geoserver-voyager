package org.geoserver.voyager;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;
import org.geotools.data.simple.SimpleFeatureReader;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Level;

import static org.geoserver.voyager.VoyagerDataStore.LOG;

public class VoyagerFeatureReader implements SimpleFeatureReader {

    final VoyagerFeatureSource source;
    final SolrClient solr;
    final SolrQuery query;

    final GeometryJSON geojson;
    final SimpleFeatureBuilder builder;

    Iterator<SolrDocument> curr;
    String cursorMark = CursorMarkParams.CURSOR_MARK_START;

    VoyagerFeatureReader(VoyagerFeatureSource source, SolrQuery query) {
        this.source = source;
        this.solr = source.store.solr;
        this.query = query;
        this.geojson = new GeometryJSON();
        this.builder = new SimpleFeatureBuilder(source.getSchema());
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return source.getSchema();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (curr != null && curr.hasNext()) return true;
        curr = null;

        query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Feature query: " + query);
        }

        try {
            QueryResponse rsp = new QueryRequest(query).process(solr);
            if (!rsp.getResults().isEmpty()) {
                curr = rsp.getResults().iterator();
                cursorMark = rsp.getNextCursorMark();
            }
        } catch (SolrServerException e) {
            throw new IOException(e);
        }

        return curr != null && curr.hasNext();
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
        SolrDocument doc = curr.next();
        VoyagerConfig config = source.store.config;

        for (AttributeDescriptor att : getFeatureType().getAttributeDescriptors()) {
            Object val = doc.get(att.getLocalName());
            if (att instanceof GeometryDescriptor) {
                val = geojson.read(val);
            }
            builder.set(att.getLocalName(), val);
        }

        String fid = doc.get(config.uniqueIdField).toString();
        return builder.buildFeature(fid);
    }

    @Override
    public void close() throws IOException {

    }
}
