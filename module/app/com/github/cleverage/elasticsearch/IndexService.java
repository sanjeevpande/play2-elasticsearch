package com.github.cleverage.elasticsearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import play.Logger;
import play.libs.F;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public abstract class IndexService {

    public static final String INDEX_DEFAULT = IndexClient.config.indexNames[0];
    public static final String PERCOLATOR_TYPE = ".percolator";

    /**
     * get indexRequest to index from a specific request
     *
     * @return
     */
    /*public static IndexRequestBuilder getIndexRequest(IndexQueryPath indexPath, String id, Index indexable) {
     *//*return new IndexRequestBuilder(IndexClient.client, IndexAction.INSTANCE)
                .setType(indexPath.type)
                .setId(id)
                .setSource(indexable.toIndex());*//*
        //IndexRequest request = new IndexRequest(indexPath.index, id);
    }*/

    /**
     * index from an request
     *
     * @param requestBuilder
     * @return
     */
    public static IndexResponse index(IndexRequestBuilder requestBuilder) {

        //IndexResponse indexResponse = requestBuilder.execute().actionGet();
        //transportClient.index(indexRequest).actionGet();
        IndexResponse indexResponse = null;
        try {
            indexResponse = IndexClient.client.index(requestBuilder.request(), RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (Logger.isDebugEnabled()) {
            Logger.debug("ElasticSearch : Index " + requestBuilder.toString());
        }
        return indexResponse;
    }

    /**
     * Create an IndexRequestBuilder
     * @param indexPath
     * @param id
     * @param indexable
     * @return
     */
    private static IndexResponse getIndexRequestBuilder(IndexQueryPath indexPath, String id, Index indexable) throws IOException {
        IndexRequest request = new IndexRequest(indexPath.index);
        IndexResponse response = IndexClient.client.index(request, RequestOptions.DEFAULT);
        return response;

        /*return IndexClient.client.prepareIndex(indexPath.index, indexPath.type, id)
                .setSource(indexable.toIndex());*/
    }

    /**
     * Add Indexable object in the index
     *
     * @param indexPath
     * @param indexable
     * @return
     */
    public static IndexResponse index(IndexQueryPath indexPath, String id, Index indexable) throws IOException {
        /*IndexResponse indexResponse = getIndexRequestBuilder(indexPath, id, indexable)
                .execute()
                .actionGet();*/
        IndexRequest request = new IndexRequest(indexPath.index);
        IndexResponse indexResponse = IndexClient.client.index(request, RequestOptions.DEFAULT);
        if (Logger.isDebugEnabled()) {
            Logger.debug("ElasticSearch : Index : " + indexResponse.getIndex() + "/" + indexResponse.getType() + "/" + indexResponse.getId() + " from " + indexable.toString());
        }
        return indexResponse;
    }

    /**
     * Add Indexable object in the index asynchronously
     *
     * @param indexPath
     * @param indexable
     * @return
     */
    public static F.Promise<IndexResponse> indexAsync(IndexQueryPath indexPath, String id, Index indexable) {
        IndexRequest request = new IndexRequest(indexPath.index, id);
        F.Promise<IndexResponse> f = null;
        IndexClient.client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                f.onRedeem((F.Callback<IndexResponse>) indexResponse);
            }

            @Override
            public void onFailure(Exception e) {
                f.onFailure((F.Callback<Throwable>) e.getCause());
            }
        });
        return f;
        //return indexAsync(getIndexRequestBuilder(indexPath, id, indexable));
    }

    /**
     * call IndexRequestBuilder on asynchronously
     * @param indexRequestBuilder
     * @return
     */
    public static F.Promise<IndexResponse> indexAsync(IndexRequestBuilder indexRequestBuilder) {
        return AsyncUtils.executeAsyncJava(indexRequestBuilder);
    }

    /**
     * Add a json document to the index
     * @param indexPath
     * @param id
     * @param json
     * @return
     */
    public static IndexResponse index(IndexQueryPath indexPath, String id, String json) throws IOException {
        //return getIndexRequestBuilder(indexPath, id, json).execute().actionGet();
        return getIndexRequestBuilder(indexPath, id, json);
    }

    /**
     * Create an IndexRequestBuilder for a Json-encoded object
     * @param indexPath
     * @param id
     * @param json
     * @return
     */
    public static IndexResponse getIndexRequestBuilder(IndexQueryPath indexPath, String id, String json) throws IOException {
        IndexRequest request = new IndexRequest(indexPath.index);
        IndexResponse indexResponse = IndexClient.client.index(request, RequestOptions.DEFAULT);
        return  indexResponse;
        //return IndexClient.client.prepareIndex(indexPath.index, indexPath.type, id).setSource(json);
    }

    /**
     * Create a BulkRequestBuilder for a List of Index objects
     * @param indexPath
     * @param indexables
     * @return
     */
    /*private static BulkRequestBuilder getBulkRequestBuilder(IndexQueryPath indexPath, List<? extends Index> indexables) {
        BulkRequestBuilder bulkRequestBuilder = IndexClient.client.prepareBulk();
        for (Index indexable : indexables) {
            bulkRequestBuilder.add(Requests.indexRequest(indexPath.index)
                    .type(indexPath.type)
                    .id(indexable.id)
                    .source(indexable.toIndex()));
        }
        return bulkRequestBuilder;
    }*/

    /**
     * Bulk index a list of indexables
     * @param indexPath
     * @param indexables
     * @return
     */
    public static BulkResponse indexBulk(IndexQueryPath indexPath, List<? extends Index> indexables) throws IOException {
        BulkRequest request = new BulkRequest(indexPath.index);
        BulkResponse response = IndexClient.client.bulk(request, RequestOptions.DEFAULT);
        return response;
        /*BulkRequestBuilder bulkRequestBuilder = getBulkRequestBuilder(indexPath, indexables);
        return bulkRequestBuilder.execute().actionGet();*/
    }

    /**
     * Bulk index a list of indexables asynchronously
     * @param indexPath
     * @param indexables
     * @return
     */
    public static F.Promise<BulkResponse> indexBulkAsync(IndexQueryPath indexPath, List<? extends Index> indexables) {
        BulkRequest request = new BulkRequest(indexPath.index);
        F.Promise<BulkResponse> f = null;
        IndexClient.client.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                f.onRedeem((F.Callback<BulkResponse>) bulkResponse);
            }

            @Override
            public void onFailure(Exception e) {
                f.onFailure((F.Callback<Throwable>) e.getCause());
            }
        });
        return f;
        //return AsyncUtils.executeAsyncJava(getBulkRequestBuilder(indexPath, indexables));
    }

    /**
     * Create a BulkRequestBuilder for a List of json-encoded objects
     * @param indexPath
     * @param jsonMap
     * @return
     */
    public static BulkRequest getBulkRequestBuilder(IndexQueryPath indexPath, Map<String, String> jsonMap) {
        //BulkRequestBuilder bulkRequestBuilder = IndexClient.client.prepareBulk();
        BulkRequest bulkRequest = new BulkRequest();
        for (String id : jsonMap.keySet()) {
            bulkRequest.add(Requests.indexRequest(indexPath.index).id(id).source(jsonMap.get(id)));
        }
        return bulkRequest;
    }

    /**
     * Bulk index a list of indexables asynchronously
     * @param bulkRequestBuilder
     * @return
     */
    public static F.Promise<BulkResponse> indexBulkAsync(BulkRequestBuilder bulkRequestBuilder) {
        return AsyncUtils.executeAsyncJava(bulkRequestBuilder);
    }

    /**
     * Create a BulkRequestBuilder for a List of IndexRequestBuilder
     * @return
     */
    /*public static BulkRequestBuilder getBulkRequestBuilder(Collection<IndexRequestBuilder> indexRequestBuilder) {
        BulkRequestBuilder bulkRequestBuilder = IndexClient.client.prepareBulk();
        for (IndexRequestBuilder requestBuilder : indexRequestBuilder) {
            bulkRequestBuilder.add(requestBuilder);
        }
        return bulkRequestBuilder;
    }*/

    /**
     * Bulk index a Map of json documents.
     * The id of the document is the key of the Map
     * @param indexPath
     * @param jsonMap
     * @return
     */
    public static BulkResponse indexBulk(IndexQueryPath indexPath, Map<String, String> jsonMap) throws IOException {
        /*BulkRequestBuilder bulkRequestBuilder = getBulkRequestBuilder(indexPath, jsonMap);
        return bulkRequestBuilder.execute().actionGet();*/
        BulkRequest request = new BulkRequest(indexPath.index);
        BulkResponse response = IndexClient.client.bulk(request, RequestOptions.DEFAULT);
        return response;
    }

    /**
     * Create an UpdateRequestBuilder
     * @param indexPath
     * @param id
     * @return
     */
    /*public static UpdateRequestBuilder getUpdateRequestBuilder(IndexQueryPath indexPath,
                                                               String id,
                                                               Map<String, Object> updateFieldValues,
                                                               String updateScript, ScriptType scriptType,
                                                               String lang) {
        return IndexClient.client.prepareUpdate(indexPath.index, indexPath.type, id)
                .setScript(new Script(scriptType, updateScript, lang, updateFieldValues));
    }*/

    /**
     * Update a document in the index
     * @param indexPath
     * @param id
     * @param updateFieldValues The fields and new values for which the update should be done
     * @param updateScript
     * @return
     */
    public static UpdateResponse update(IndexQueryPath indexPath,
                                        String id,
                                        Map<String, Object> updateFieldValues,
                                        String updateScript, ScriptType scriptType,
                                        String lang) throws IOException {
        /*return getUpdateRequestBuilder(indexPath, id, updateFieldValues, updateScript, scriptType, lang)
                .execute()
                .actionGet();*/
        UpdateRequest request = new UpdateRequest(indexPath.index, id);
        UpdateResponse response = IndexClient.client.update(request, RequestOptions.DEFAULT);
        return  response;
    }

    /**
     * Update a document asynchronously
     * @param indexPath
     * @param id
     * @param updateFieldValues The fields and new values for which the update should be done
     * @param updateScript
     * @return
     */
    public static F.Promise<UpdateResponse> updateAsync(IndexQueryPath indexPath,
                                                        String id,
                                                        Map<String, Object> updateFieldValues,
                                                        String updateScript, ScriptType scriptType,
                                                        String lang) {
        UpdateRequest request = new UpdateRequest(indexPath.index, id);
        F.Promise<UpdateResponse> f = null;
        IndexClient.client.updateAsync(request, RequestOptions.DEFAULT,  new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                f.onRedeem((F.Callback<UpdateResponse>) updateResponse);
            }

            @Override
            public void onFailure(Exception e) {
                f.onFailure((F.Callback<Throwable>) e.getCause());
            }
        });
        return f;
        //return updateAsync(getUpdateRequestBuilder(indexPath, id, updateFieldValues, updateScript, scriptType, lang));
    }

    /**
     * Call update asynchronously
     * @param updateRequestBuilder
     * @return
     */
    public static F.Promise<UpdateResponse> updateAsync(UpdateRequestBuilder updateRequestBuilder) {
        return AsyncUtils.executeAsyncJava(updateRequestBuilder);
    }

    /**
     * Create a DeleteRequestBuilder
     * @param indexPath
     * @param id
     * @return
     */
    public static DeleteResponse getDeleteRequestBuilder(IndexQueryPath indexPath, String id) {
        DeleteRequest request = new DeleteRequest(indexPath.index, id);
        DeleteResponse response = null;
        try {
            response = IndexClient.client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
        //return IndexClient.client.prepareDelete(indexPath.index, indexPath.type, id);
    }

    /**
     * Delete element in index asynchronously
     * @param indexPath
     * @return
     */
    public static F.Promise<DeleteResponse> deleteAsync(IndexQueryPath indexPath, String id) {
        DeleteRequest request = new DeleteRequest(indexPath.index, id);
        F.Promise<DeleteResponse> f = null;
        IndexClient.client.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                f.onRedeem((F.Callback<DeleteResponse>) deleteResponse);
            }

            @Override
            public void onFailure(Exception e) {
                f.onFailure((F.Callback<Throwable>) e.getCause());
            }
        });
        return f;
        //return AsyncUtils.executeAsyncJava(getDeleteRequestBuilder(indexPath, id));
    }

    /**
     * Delete element in index
     * @param indexPath
     * @return
     */
    public static DeleteResponse delete(IndexQueryPath indexPath, String id) {
        /*DeleteResponse deleteResponse = getDeleteRequestBuilder(indexPath, id)
                .execute()
                .actionGet();*/
        DeleteRequest request = new DeleteRequest(indexPath.index, id);
        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = IndexClient.client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (Logger.isDebugEnabled()) {
            Logger.debug("ElasticSearch : Delete " + deleteResponse.toString());
        }

        return deleteResponse;
    }

    /**
     * Create a GetRequestBuilder
     * @param indexPath
     * @param id
     * @return
     */
    /*public static GetRequestBuilder getGetRequestBuilder(IndexQueryPath indexPath, String id) {
        return IndexClient.client.prepareGet(indexPath.index, indexPath.type, id);
    }*/

    /**
     * Create a MultiGetRequestBuilder
     * @param indexPath
     * @param ids
     * @return
     */

    public static MultiGetRequest getMultiGetRequestBuilder(IndexQueryPath indexPath, Collection<String> ids) {
        //MultiGetRequestBuilder multiGetRequestBuilder = IndexClient.client.prepareMultiGet();
        MultiGetRequest request = new MultiGetRequest();
        for (String id : ids) {
            request.add(indexPath.index, id);
        }
        return request;
    }

    /**
     * Get the json representation of a document from an id
     * @param indexPath
     * @param id
     * @return
     */
    public static String getAsString(IndexQueryPath indexPath, String id) throws IOException {
        GetRequest request = new GetRequest(indexPath.index, id);
        GetResponse response = IndexClient.client.get(request, RequestOptions.DEFAULT);
        return  response.getSourceAsString();
        /*return getGetRequestBuilder(indexPath, id)
          .execute()
          .actionGet()
          .getSourceAsString();*/
    }

    private static <T extends Index> T getTFromGetResponse(Class<T> clazz, GetResponse getResponse) {
        T t = IndexUtils.getInstanceIndex(clazz);
        if (!getResponse.isExists()) {
            return null;
        }

        // Create a new Indexable Object for the return
        Map<String, Object> map = getResponse.getSourceAsMap();

        t = (T) t.fromIndex(map);
        t.id = getResponse.getId();
        return t;
    }

    private static <T extends Index> List<T> getTsFromMultiGetResponse(Class<T> clazz, MultiGetResponse multiGetResponse) {
        T t = IndexUtils.getInstanceIndex(clazz);
        MultiGetItemResponse[] multiGetItemResponses = multiGetResponse.getResponses();
        if (multiGetItemResponses.length == 0) {
            return null;
        }

        List<T> ts = new ArrayList<>(multiGetItemResponses.length);
        for (MultiGetItemResponse multiGetItemResponse : multiGetItemResponses) {
            Map<String, Object> map = multiGetItemResponse.getResponse().getSourceAsMap();

            t = (T) t.fromIndex(map);
            t.id = multiGetItemResponse.getResponse().getId();
            ts.add(t);
        }

        return ts;
    }

    /**
     * Get Indexable Object for an Id
     *
     * @param indexPath
     * @param clazz
     * @return
     */
    public static <T extends Index> T get(IndexQueryPath indexPath, Class<T> clazz, String id) throws IOException {
        /*GetRequestBuilder getRequestBuilder = getGetRequestBuilder(indexPath, id);
        GetResponse getResponse = getRequestBuilder.execute().actionGet();*/
        GetRequest request = new GetRequest(indexPath.index, id);
        GetResponse getResponse = IndexClient.client.get(request, RequestOptions.DEFAULT);
        return getTFromGetResponse(clazz, getResponse);
    }

    /**
     * Get Indexable Object for an Id asynchronously
     * @param indexPath
     * @param clazz
     * @param id
     * @param <T>
     * @return
     */
    public static <T extends Index> F.Promise<T> getAsync(IndexQueryPath indexPath, final Class<T> clazz, String id) {
        /*F.Promise<GetResponse> responsePromise = AsyncUtils.executeAsyncJava(getGetRequestBuilder(indexPath, id));
        return responsePromise.map(
          new F.Function<GetResponse, T>() {
              public T apply(GetResponse getResponse) {
                  return getTFromGetResponse(clazz, getResponse);
              }
          }
        );*/
        GetRequest request = new GetRequest(indexPath.index, id);
        F.Promise<T> f = null;
        IndexClient.client.getAsync(request, RequestOptions.DEFAULT,  new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                f.onRedeem((F.Callback<T>) getResponse);
            }

            @Override
            public void onFailure(Exception e) {
                f.onFailure((F.Callback<Throwable>) e.getCause());
            }
        });
        return f;
    }

    /**
     * Get a reponse for a simple request
     * @param indexName
     * @param indexType
     * @param id
     * @return
     */
    public static GetResponse get(String indexName, String indexType, String id) throws IOException {
        GetRequest request = new GetRequest(indexName, id);
        GetResponse getResponse = IndexClient.client.get(request, RequestOptions.DEFAULT);
        /*GetRequestBuilder getRequestBuilder = IndexClient.client.prepareGet(indexName, indexType, id);
        GetResponse getResponse = getRequestBuilder.execute().actionGet();*/
        return getResponse;
    }

    /**
     * Get Indexable Object for an Id
     *
     * @param indexPath
     * @param clazz
     * @return
     */
    public static <T extends Index> List<T> multiGet(IndexQueryPath indexPath, Class<T> clazz, Collection<String> ids) throws IOException {
        MultiGetRequest request = getMultiGetRequestBuilder(indexPath, ids);
        MultiGetResponse multiGetResponse = IndexClient.client.mget(request, RequestOptions.DEFAULT);
        return getTsFromMultiGetResponse(clazz, multiGetResponse);
    }

    /**
     * Get Indexable Object for an Ids asynchronously
     * @param indexPath
     * @param clazz
     * @param ids
     * @param <T>
     * @return
     */
    public static <T extends Index> F.Promise<List<T>> multiGetAsync(IndexQueryPath indexPath, final Class<T> clazz, Collection<String> ids) {
        MultiGetRequest request = getMultiGetRequestBuilder(indexPath, ids);
        F.Promise<List<T>> f = null;
        IndexClient.client.mgetAsync(request, RequestOptions.DEFAULT, new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse multiGetItemResponses) {
                f.onRedeem((F.Callback<List<T>>) multiGetItemResponses);
            }

            @Override
            public void onFailure(Exception e) {
                f.onFailure((F.Callback<Throwable>) e.getCause());
            }
        });
        return f;
        /*F.Promise<MultiGetResponse> responsePromise  = AsyncUtils.executeAsyncJava(multiGetRequestBuilder);
        return responsePromise.filter(null).map(
          new F.Function<MultiGetResponse, List<T>>() {
              public List<T> apply(MultiGetResponse getResponse) {
                  return getTsFromMultiGetResponse(clazz, getResponse);
              }
          }
        );*/
    }

    /**
     * Get a response for a simple request
     * @param indexName
     * @param indexType
     * @param ids
     * @return
     */
    public static MultiGetResponse multiGet(String indexName, String indexType, Collection<String> ids) throws IOException {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (String id : ids) {
            multiGetRequest.add(indexName, id);
        }
        MultiGetResponse multiGetResponse = IndexClient.client.mget(multiGetRequest, RequestOptions.DEFAULT);
        return multiGetResponse;
    }

    /**
     * Search information on Index from a query
     * @param indexQuery
     * @param <T>
     * @return
     */
    public static <T extends Index> IndexResults<T> search(IndexQueryPath indexPath, IndexQuery<T> indexQuery) {
        return indexQuery.fetch(indexPath);
    }

    /**
     * Search asynchronously information on Index from a query
     * @param indexPath
     * @param indexQuery
     * @param <T>
     * @return
     */
    public static <T extends Index> F.Promise<IndexResults<T>> searchAsync(IndexQueryPath indexPath,
                                                                           IndexQuery<T> indexQuery,
                                                                           QueryBuilder filter) {
        return indexQuery.fetchAsync(indexPath, filter);
    }

    /**
     * Test if an indice Exists
     * @return true if exists
     */
    public static boolean existsIndex(String indexName) throws IOException {

        //AdminClient admin = client.admin();
        //IndicesAdminClient indices = admin.indices();
        //IndicesExistsRequestBuilder indicesExistsRequestBuilder = indices.prepareExists(indexName);
        //IndicesExistsResponse response = indicesExistsRequestBuilder.execute().actionGet();
        //RestHighLevelClient client = IndexClient.client;
        //IndicesClient indicesClient = client.indices();
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        return IndexClient.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        //return response.isExists();
    }

    /**
     * Create the index
     */
    public static void createIndex(String indexName) {
        Logger.debug("ElasticSearch : creating index [" + indexName + "]");
        try {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            IndexClient.client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            /*CreateIndexRequestBuilder creator = IndexClient.client.admin().indices().prepareCreate(indexName);
            String setting = IndexClient.config.indexSettings.get(indexName);
            if (setting != null) {
                creator.setSettings(setting);
            }
            creator.execute().actionGet();*/
        } catch (Exception e) {
            Logger.error("ElasticSearch : Index create error : " + e.toString());
        }
    }

    /**
     * Delete the index
     */
    public static void deleteIndex(String indexName) {
        Logger.debug("ElasticSearch : deleting index [" + indexName + "]");
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            IndexClient.client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            //IndexClient.client.admin().indices().prepareDelete(indexName).execute().actionGet();
        } catch (IndexNotFoundException indexMissing) {
            Logger.debug("ElasticSearch : Index " + indexName + " no exists");
        } catch (Exception e) {
            Logger.error("ElasticSearch : Index drop error : " + e.toString());
        }
    }

    /**
     * Create Mapping ( for example mapping type : nested, geo_point  )
     * see http://www.elasticsearch.org/guide/reference/mapping/
     * <p/>
     * {
     * "tweet" : {
     * "properties" : {
     * "message" : {"type" : "string", "store" : "yes"}
     * }
     * }
     * }
     *
     * @param indexName
     * @param indexType
     * @param indexMapping
     */
    public static AcknowledgedResponse createMapping(String indexName, String indexType, String indexMapping) throws IOException {
        Logger.debug("ElasticSearch : creating mapping [" + indexName + "/" + indexType + "] :  " + indexMapping);
        //AcknowledgedResponse response = IndexClient.client.admin().indices().preparePutMapping(indexName).setType(indexType).setSource(indexMapping).execute().actionGet();
        PutMappingRequest request = new PutMappingRequest(indexName);
        AcknowledgedResponse response = IndexClient.client.indices().putMapping(request, RequestOptions.DEFAULT);
        return response;
    }

    /**
     * Read the Mapping for a type
     * @param indexType
     * @return
     */
    public static String getMapping(String indexName, String indexType) {
        /*ClusterState state = IndexClient.client.admin().cluster()
                .prepareState()
                .setIndices(IndexService.INDEX_DEFAULT)
                .execute().actionGet().getState();
        MappingMetaData mappingMetaData = state.getMetaData().index(indexName).mapping();
        if (mappingMetaData != null) {
            return mappingMetaData.source().toString();
        } else {
            return null;
        }*/
        return  null;
    }

    /**
     * call createMapping for list of @indexType
     * @param indexName
     */
    public static void prepareIndex(String indexName) throws IOException {

        Map<IndexQueryPath, String> indexMappings = IndexClient.config.indexMappings;
        for (IndexQueryPath indexQueryPath : indexMappings.keySet()) {

            if(indexName != null && indexName.equals(indexQueryPath.index)) {
                String indexType = indexQueryPath.type;
                String indexMapping = indexMappings.get(indexQueryPath);
                if (indexMapping != null) {
                    createMapping(indexName, indexType, indexMapping);
                }
            }
        }
    }

    public static void cleanIndex() throws IOException {

        String[] indexNames = IndexClient.config.indexNames;
        for (String indexName : indexNames) {
            cleanIndex(indexName);
        }
    }

    public static void cleanIndex(String indexName) throws IOException {

        if (IndexService.existsIndex(indexName)) {
            IndexService.deleteIndex(indexName);
        }
        IndexService.createIndex(indexName);
        IndexService.prepareIndex(indexName);
    }

    /**
     * Refresh full index
     */
    public static void refresh() throws IOException {
        String[] indexNames = IndexClient.config.indexNames;
        for (String indexName : indexNames) {
            refresh(indexName);
        }
    }

    /**
     * Refresh an index
     * @param indexName
     */
    private static void refresh(String indexName) throws IOException {
        RefreshRequest refreshRequest = new RefreshRequest(indexName);
        IndexClient.client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
        //IndexClient.client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
    }

    /**
     * Flush full index
     */
    public static void flush() throws IOException {
        String[] indexNames = IndexClient.config.indexNames;
        for (String indexName : indexNames) {
            flush(indexName);
        }
    }

    /**
     * Flush an index
     * @param indexName
     */
    public static void flush(String indexName) throws IOException {
        FlushRequest flushRequest = new FlushRequest(indexName);
        IndexClient.client.indices().flush(flushRequest, RequestOptions.DEFAULT);
        //IndexClient.client.admin().indices().flush(new FlushRequest(indexName)).actionGet();
    }

    /**
     * Create Percolator from a queryBuilder
     *
     * @param namePercolator
     * @param queryBuilder
     * @return
     */
    /*public static IndexResponse createPercolator(String namePercolator, QueryBuilder queryBuilder) {
        return createPercolator(INDEX_DEFAULT, namePercolator, queryBuilder);
    }*/

    /*public static IndexResponse createPercolator(String indexName, String queryName, QueryBuilder queryBuilder) {
        XContentBuilder source = null;
        try {
            source = jsonBuilder().startObject()
                    .field("query", queryBuilder)
                    .endObject();
        } catch (IOException e) {
            Logger.error("Elasticsearch : Error when creating percolator from a queryBuilder", e);
        }

        IndexRequestBuilder percolatorRequest =
                IndexClient.client.prepareIndex(indexName,
                        PERCOLATOR_TYPE,
                        queryName)
                        .setSource(source)
                        .setRefreshPolicy(RefreshPolicy.NONE);

        return percolatorRequest.execute().actionGet();
    }*/

    /**
     * Create Percolator
     *
     * @param namePercolator
     * @param query
     * @return
     * @throws IOException
     */
    /*public static IndexResponse createPercolator(String namePercolator, String query) {
        return createPercolator(INDEX_DEFAULT,namePercolator,query);
    }*/

    /*public static IndexResponse createPercolator(String indexName, String queryName, String query) {
        IndexRequestBuilder percolatorRequest =
                IndexClient.client.prepareIndex(indexName,
                        PERCOLATOR_TYPE,
                        queryName)
                        .setSource("{\"query\": " + query + "}")
                        .setRefreshPolicy(RefreshPolicy.NONE);

        return percolatorRequest.execute().actionGet();
    }*/


    /**
     * Check if a percolator exists
     * @param namePercolator
     * @return
     */
    /*public static boolean percolatorExists(String namePercolator) {
        return percolatorExistsInIndex(namePercolator, INDEX_DEFAULT);
    }*/

    /*public static boolean percolatorExistsInIndex(String namePercolator, String indexName){
        try {
            GetResponse responseExist = IndexService.getPercolator(indexName, namePercolator);
            return (responseExist.isExists());
        } catch (IndexNotFoundException e) {
            return false;
        }
    }*/

    /**
     * Delete Percolator
     *
     * @param namePercolator
     * @return
     */
    /*public static DeleteResponse deletePercolator(String namePercolator) {
        return deletePercolator(IndexService.INDEX_DEFAULT, namePercolator);
    }*/

    /*public static DeleteResponse deletePercolator(String indexName, String namePercolator) {
        return delete(new IndexQueryPath(indexName, PERCOLATOR_TYPE), namePercolator);
    }*/


    // See important notes section on http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-percolate.html
    /*/**
     * Delete all percolators
     *//*
    public static void deletePercolators() {
        try {
            DeleteIndexResponse deleteIndexResponse = IndexClient.client.admin().indices().prepareDelete(INDEX_PERCOLATOR).execute().actionGet();
            if(!deleteIndexResponse.isAcknowledged()){
                throw new Exception(" no acknowledged");
            }
        } catch (IndexNotFoundException indexMissing) {
            Logger.debug("ElasticSearch : Index " + INDEX_PERCOLATOR + " no exists");
        } catch (Exception e) {
            Logger.error("ElasticSearch : Index drop error : " + e.toString());
        }
    }*/

    /**
     * Get the percolator details
     * @param queryName
     * @return
     */
    /*public static GetResponse getPercolator(String queryName) {
        return getPercolator(INDEX_DEFAULT, queryName);
    }*/

    /**
     * Get the percolator details on an index
     * @param indexName
     * @param queryName
     * @return
     */
    /*public static GetResponse getPercolator(String indexName, String queryName){
        return get(indexName, PERCOLATOR_TYPE, queryName);
    }*/

    /**
     * Get percolator match this Object
     *
     * @param indexable
     * @return
     * @throws IOException
     */
    public static List<String> getPercolatorsForDoc(Index indexable) { /*

        PercolateRequestBuilder percolateRequestBuilder = new PercolateRequestBuilder(IndexClient.client, PercolateAction.INSTANCE);
        percolateRequestBuilder.setDocumentType(indexable.getIndexPath().type);

        XContentBuilder doc = null;
        try {
            doc = jsonBuilder().startObject().startObject("doc").startObject(indexable.getIndexPath().type);
            Map<String, Object> map = indexable.toIndex();
            for (String key : map.keySet()) {
                if (key != null && map.get(key) != null) {
                    doc.field(key, map.get(key));
                }
            }
            doc.endObject().endObject().endObject();
        } catch (Exception e) {
            Logger.debug("Elasticsearch : Error when get percolator for ");
        }

        percolateRequestBuilder.setSource(doc);

        PercolateResponse percolateResponse = percolateRequestBuilder.execute().actionGet();
        if (percolateResponse == null) {
            return null;
        }
        List<String> matchedQueryIds = new ArrayList<String>();
        List<PercolateResponse.Match> matches = percolateResponse.getMatches();
        for(PercolateResponse.Match match : matches){
            matchedQueryIds.add(match.getId().string());
        }
        return matchedQueryIds;*/
        return null;
    }
}