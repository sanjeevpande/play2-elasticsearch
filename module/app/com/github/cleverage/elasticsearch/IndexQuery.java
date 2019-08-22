package com.github.cleverage.elasticsearch;

import org.apache.commons.lang3.Validate;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import play.Logger;
import play.libs.F;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An ElasticSearch query
 *
 * @param <T> extends Index
 */
public class IndexQuery<T extends Index> {

    /**
     * Objet retourné dans les résultats
     */
    private final Class<T> clazz;

    /**
     * Query searchRequestBuilder
     */
    private QueryBuilder builder = QueryBuilders.matchAllQuery();
    private String query = null;
    private List<AbstractAggregationBuilder> aggregations = new ArrayList<AbstractAggregationBuilder>();
    private List<SortBuilder> sorts = new ArrayList<SortBuilder>();

    private int from = -1;
    private int size = -1;
    private boolean explain = false;
    private boolean noField = false;
    private String preference = null;
    private String route = null;

    public IndexQuery(Class<T> clazz) {
        Validate.notNull(clazz, "clazz cannot be null");
        this.clazz = clazz;
    }

    public IndexQuery<T> setBuilder(QueryBuilder builder) {
        this.builder = builder;

        return this;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setNoField(boolean noField) {
        this.noField = noField;
    }

    /**
     * Sets from
     *
     * @param from
     *            record index to start from
     * @return self
     */
    public IndexQuery<T> from(int from) {
        this.from = from;

        return this;
    }

    /**
     * Sets fetch size
     *
     * @param size
     *            the fetch size
     * @return self
     */
    public IndexQuery<T> size(int size) {
        this.size = size;

        return this;
    }

    public IndexQuery<T> setExplain(boolean explain) {
        this.explain = explain;

        return this;
    }

    /**
     * Sets a preference of which shard replicas to execute the search request on.
     * @param preference
     *
     * @return
     */
    public IndexQuery<T> setPreference(String preference) {
        this.preference = preference;

        return this;
    }

    /**
     * Sets a route of which shard to execute the search request against.
     * Only use this if path is already set in indexMapping
     * @param route
     *
     * @return
     */
    public IndexQuery<T> setRoute(String route) {
        this.route = route;

        return this;
    }

    /**
     * Adds an aggregation
     *
     * @param aggregation
     *            the aggregation
     * @return self
     */
    public IndexQuery<T> addAggregation(AbstractAggregationBuilder aggregation) {
        Validate.notNull(aggregation, "aggregation cannot be null");
        aggregations.add(aggregation);

        return this;
    }

    /**
     * Sorts the result by a specific field
     *
     * @param field
     *            the sort field
     * @param order
     *            the sort order
     * @return self
     */
    public IndexQuery<T> addSort(String field, SortOrder order) {
        Validate.notEmpty(field, "field cannot be null");
        Validate.notNull(order, "order cannot be null");
        sorts.add(SortBuilders.fieldSort(field).order(order));

        return this;
    }

    /**
     * Adds a generic {@link SortBuilder}
     *
     * @param sort
     *            the sort searchRequestBuilder
     * @return self
     */
    public IndexQuery<T> addSort(SortBuilder sort) {
        Validate.notNull(sort, "sort cannot be null");
        sorts.add(sort);

        return this;
    }

    /**
     * Runs the query
     * @param indexQueryPath
     * @return
     */
    public IndexResults<T> fetch(IndexQueryPath indexQueryPath) {
        return fetch(indexQueryPath, null);
    }

    /**
     * Runs the query with a filter
     * @param indexQueryPath
     * @param filter
     * @return
     */
    public IndexResults<T> fetch(IndexQueryPath indexQueryPath, QueryBuilder filter) {

        SearchRequest searchRequest = new SearchRequest(indexQueryPath.index);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(filter);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = IndexClient.client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        IndexResults<T> searchResults = toSearchResults(searchResponse);

        return searchResults;

        /*SearchRequestBuilder request = getSearchRequestBuilder(indexQueryPath, filter);
        return executeSearchRequest(request);*/
    }

    /**
     * Runs the query asynchronously
     * @param indexQueryPath
     * @return
     */
    public F.Promise<IndexResults<T>> fetchAsync(IndexQueryPath indexQueryPath) {
        return fetchAsync(indexQueryPath, null);
    }

    /**
     * Runs the query asynchronously with a filter
     * @param indexQueryPath
     * @param filter
     * @return
     */
    public F.Promise<IndexResults<T>> fetchAsync(IndexQueryPath indexQueryPath, QueryBuilder filter) {
        /*SearchRequestBuilder request = getSearchRequestBuilder(indexQueryPath, filter);
        F.Promise<SearchResponse> searchResponsePromise = AsyncUtils.executeAsyncJava(request);
        return searchResponsePromise.map(new F.Function<SearchResponse, IndexResults<T>>() {
            @Override
            public IndexResults<T> apply(SearchResponse searchResponse) {
                return toSearchResults(searchResponse);
            }
        });*/
        return null;
    }

    public IndexResults<T> executeSearchRequest(SearchRequestBuilder request) {

        SearchResponse searchResponse = request.execute().actionGet();

        if (IndexClient.config.showRequest) {
            Logger.debug("ElasticSearch : Response -> " + searchResponse.toString());
        }

        IndexResults<T> searchResults = toSearchResults(searchResponse);

        return searchResults;
    }

    public SearchRequestBuilder getSearchRequestBuilder(IndexQueryPath indexQueryPath){
        //return getSearchRequestBuilder(indexQueryPath, null);
        return null;
    }

    public SearchRequestBuilder getSearchRequestBuilder(IndexQueryPath indexQueryPath, QueryBuilder filter) throws IOException {

        /*SearchRequest searchRequest = new SearchRequest(indexQueryPath.index);
        searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
        IndexClient.client.search(searchRequest, RequestOptions.DEFAULT);*/

        // Build request
        /*SearchRequestBuilder request = IndexClient.client
                .prepareSearch(indexQueryPath.index)
                .setTypes(indexQueryPath.type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPostFilter(filter);

        // set Query
        if (StringUtils.isNotBlank(query)) {
            //request.setQuery(query);
            request.setQuery(builder);
        }
        else
        {
            request.setQuery(builder);
        }

        // set no Fields -> only return id and type
        if(noField) {
            request.setFetchSource(false);
        }

        // Aggregations
        for (AbstractAggregationBuilder aggregation : aggregations) {
            request.addAggregation(aggregation);
        }

        // Sorting
        for (SortBuilder sort : sorts) {
            request.addSort(sort);
        }

        // Paging
        if (from > -1) {
            request.setFrom(from);
        }
        if (size > -1) {
            request.setSize(size);
        }

        // Explain
        if (explain) {
            request.setExplain(true);
        }

        if (preference != null) {
            request.setPreference(preference);
        }

        if(route != null){
            request.setRouting(route);
        }

        if (IndexClient.config.showRequest) {
            if (StringUtils.isNotBlank(query)) {
                Logger.debug("ElasticSearch : Query -> " + query);
            }
            else
            {
                Logger.debug("ElasticSearch : Query -> "+ builder.toString());
            }
        }
        return request;*/
        return null;
    }

    private IndexResults<T> toSearchResults(SearchResponse searchResponse) {
        // Get Total Records Found
        long count = searchResponse.getHits().getTotalHits().value;

        // Get Aggregations
        Aggregations aggregationsResponse = searchResponse.getAggregations();

        // Get List results
        List<T> results = new ArrayList<T>();

        // Loop on each one
        for (SearchHit h : searchResponse.getHits()) {

            // Get Data Map
            Map<String, Object> map = h.getSourceAsMap();

            // Create a new Indexable Object for the return
            T objectIndexable = IndexUtils.getInstanceIndex(clazz);
            T t = (T) objectIndexable.fromIndex(map);
            t.id = h.getId();
            t.searchHit = h;

            results.add(t);
        }

        Logger.info("ElasticSearch : ES Results -> "+ results.toString());

        if(Logger.isDebugEnabled()) {
            Logger.debug("ElasticSearch : Results -> "+ results.toString());
        }

        // pagination
        long pageSize = 10;
        if (size > -1) {
            pageSize = size;
        }

        long pageCurrent = 1;
        if(from > 0) {
            pageCurrent = ((int) (from / pageSize))+1;
        }

        long pageNb;
        if (pageSize == 0) {
            pageNb = 1;
        } else {
            pageNb = (long)Math.ceil(new BigDecimal(count).divide(new BigDecimal(pageSize), 2, RoundingMode.HALF_UP).doubleValue());
        }

        // Return Results
        return new IndexResults<T>(count, pageSize, pageCurrent, pageNb, results, aggregationsResponse);
    }

}
