package org.geoserver.voyager;

import com.google.common.base.Strings;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.geotools.data.Query;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.filter.FilterCapabilities;
import org.geotools.filter.visitor.SimplifyingFilterVisitor;
import org.geotools.util.factory.Hints;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;


public class VoyagerDataStore extends ContentDataStore  {

    final static Logger LOG = Logging.getLogger("voyager");

    final VoyagerConfig config;
    final SolrClient solr;

    public VoyagerDataStore(VoyagerConfig config) {
        this.config = config;
        this.solr = buildSolrClient(config);
        setGeometryFactory(new GeometryFactory());
    }

    SolrClient buildSolrClient(VoyagerConfig config) {
        String uri = config.solrUri();
        if (uri.startsWith("http")) {
            // TODO: make this configurable
            HttpSolrClient solr = new HttpSolrClient.Builder()
                .withBaseSolrUrl(uri)
                .allowCompression(true)
                .withConnectionTimeout(config.timeout)
                .withSocketTimeout(config.timeout)
                .build();
            solr.setFollowRedirects(true);
            return solr;
        }
        else {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        // TODO: add other indexes?
        return Collections.singletonList(new NameImpl(namespaceURI, config.index));
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new VoyagerFeatureSource(ensureEntry(entry.getName()), this);
    }

    public FilterCapabilities filterCapabilities() {
        return new FilterToSolr(null, config).getCapabilities();
    }

    SolrQuery query(SimpleFeatureType schema, Query q) throws Exception {
        SolrQuery query = new SolrQuery();
        query.setParam("omitHeader", true);
        query.addField("*");
        query.addField(config.geoField + ":[geo]");

        // Column names
        if (q.getPropertyNames() != null) {
            for (String prop : q.getPropertyNames()) {
                if (config.includesField(prop)) query.addField(prop);
            }
        }
        query.setQuery("*:*");

        for (String fq : config.filters) {
            query.addFilterQuery(fq);
        }

        // limit / offset, we don't use solrs start + rows since we use cursors
        // for paging, so we use a wrapping reader to implement start/offset function
        // Encode limit/offset, if necessary
        query.setRows(config.pageSize);

        // Sort
        SolrQuery.ORDER naturalSortOrder = SolrQuery.ORDER.asc;
        if (q.getSortBy() != null) {
            for (SortBy sort : q.getSortBy()) {
                if (sort.getPropertyName() != null) {
                    query.addSort(
                            sort.getPropertyName().getPropertyName(),
                            sort.getSortOrder().equals(SortOrder.ASCENDING)
                                    ? SolrQuery.ORDER.asc
                                    : SolrQuery.ORDER.desc);
                } else {
                    naturalSortOrder =
                            sort.getSortOrder().equals(SortOrder.ASCENDING)
                                    ? SolrQuery.ORDER.asc
                                    : SolrQuery.ORDER.desc;
                }
            }
        }

        // Always add natural sort by PK to support pagination
        query.addSort(config.uniqueIdField, naturalSortOrder);

        // Encode OGC filer
        FilterToSolr f2s = new FilterToSolr(schema, config);

        Filter simplified = SimplifyingFilterVisitor.simplify(q.getFilter(), schema);
        String fq = f2s.encodeToString(simplified);
        if (fq != null && !fq.isEmpty()) {
            query.addFilterQuery(fq);
        }

        // view params
        Map<String,String> viewParams = (Map<String, String>) q.getHints().get(Hints.VIRTUAL_TABLE_PARAMETERS);
        if (viewParams != null && !viewParams.isEmpty()) {
            parseFilterFromViewParams(viewParams).ifPresent(filters -> {
                filters.forEach(query::addFilterQuery);
            });
        }

        return query;
    }

    Optional<List<String>> parseFilterFromViewParams(Map<String, String> viewParams) {
        String fq = viewParams.get("FQ");
        if (!Strings.isNullOrEmpty(fq)) {
            String[] parts = fq.split("\\s*\\|\\s*");
            List<String> filters = new ArrayList<>(parts.length);
            for (String p : parts) {
                String[] kv = p.split("\\s*->\\s*", 2);
                if (kv.length > 1) {
                    filters.add(kv[0] + ":" + kv[1]);
                }
                else {
                    LOG.warning("Illegal view param specified: " + p);
                }
            }

            return Optional.of(filters).filter(it -> !it.isEmpty());
        }
        return Optional.empty();
    }

    @Override
    public void dispose() {
        super.dispose();
        try {
            solr.close();
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Error closing solr client", e);
        }
    }
}
