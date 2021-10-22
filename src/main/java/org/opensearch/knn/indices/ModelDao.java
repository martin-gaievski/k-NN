/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.knn.indices;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteAction;
import org.opensearch.action.delete.DeleteRequestBuilder;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequestBuilder;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.util.KNNEngine;
import org.opensearch.knn.plugin.transport.UpdateModelMetadataAction;
import org.opensearch.knn.plugin.transport.UpdateModelMetadataRequest;

import java.io.IOException;
import java.net.URL;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.opensearch.knn.common.KNNConstants.MODEL_INDEX_MAPPING_PATH;
import static org.opensearch.knn.common.KNNConstants.MODEL_INDEX_NAME;
import static org.opensearch.knn.index.KNNSettings.MODEL_INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.knn.index.KNNSettings.MODEL_INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.opensearch.knn.common.KNNConstants.MODEL_METADATA_FIELD;

/**
 * ModelDao is used to interface with the model persistence layer
 */
public interface ModelDao {

    /**
     * Creates model index. It is possible that the 2 threads call this function simulateously. In this case, one
     * thread will throw a ResourceAlreadyExistsException. This should be caught and handled.
     *
     * @param actionListener CreateIndexResponse listener
     * @throws IOException thrown when get mapping fails
     */
    void create(ActionListener<CreateIndexResponse> actionListener) throws IOException;

    /**
     * Checks if the model index exists
     *
     * @return true if the model index exists; false otherwise
     */
    boolean isCreated();

    /**
     * Put a model into the system index. Non-blocking
     *
     * @param modelId   Id of model to create
     * @param model Model to be indexed
     * @param listener  handles acknowledged response
     */
    void put(String modelId, Model model, ActionListener<AcknowledgedResponse> listener) throws IOException;

    /**
     * Put a model into the system index. Non-blocking. When no id is passed in, OpenSearch will generate the id
     * automatically. The id can be retrieved in the IndexResponse.
     *
     * @param model Model to be indexed
     * @param listener  handles acknowledged response
     */
    void put(Model model, ActionListener<AcknowledgedResponse> listener) throws IOException;

    /**
     * Get a model from the system index. Call blocks.
     *
     * @param modelId to retrieve
     * @return model
     * @throws ExecutionException   thrown on search
     * @throws InterruptedException thrown on search
     */
    Model get(String modelId) throws ExecutionException, InterruptedException;

    /**
     * Get metadata for a model. Non-blocking.
     *
     * @param modelId to retrieve
     * @return modelMetadata
     */
    ModelMetadata getMetadata(String modelId);

    /**
     * Delete model from index
     *
     * @param modelId  to delete
     * @param listener handles delete response
     */
    void delete(String modelId, ActionListener<DeleteResponse> listener);

    /**
     * Implementation of ModelDao for k-NN model index
     */
    final class OpenSearchKNNModelDao implements ModelDao {

        public static Logger logger = LogManager.getLogger(ModelDao.class);

        private int numberOfShards;
        private int numberOfReplicas;

        private static OpenSearchKNNModelDao INSTANCE;
        private static Client client;
        private static ClusterService clusterService;
        private static Settings settings;

        /**
         * Make sure we just have one instance of model index
         *
         * @return ModelIndex instance
         */
        public static synchronized OpenSearchKNNModelDao getInstance() {
            if (INSTANCE == null) {
                INSTANCE = new OpenSearchKNNModelDao();
            }
            return INSTANCE;
        }

        public static void initialize(Client client, ClusterService clusterService, Settings settings) {
            OpenSearchKNNModelDao.client = client;
            OpenSearchKNNModelDao.clusterService = clusterService;
            OpenSearchKNNModelDao.settings = settings;
        }

        private OpenSearchKNNModelDao() {
            numberOfShards = MODEL_INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
            numberOfReplicas = MODEL_INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);

            clusterService.getClusterSettings().addSettingsUpdateConsumer(MODEL_INDEX_NUMBER_OF_SHARDS_SETTING,
                    it -> numberOfShards = it);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MODEL_INDEX_NUMBER_OF_REPLICAS_SETTING,
                    it -> numberOfReplicas = it);
        }

        @Override
        public void create(ActionListener<CreateIndexResponse> actionListener) throws IOException {
            if (isCreated()) {
                return;
            }

            CreateIndexRequest request = new CreateIndexRequest(MODEL_INDEX_NAME)
                    .mapping("_doc", getMapping(), XContentType.JSON)
                    .settings(Settings.builder()
                            .put("index.hidden", true)
                            .put("index.number_of_shards", this.numberOfShards)
                            .put("index.number_of_replicas", this.numberOfReplicas)
                    );
            client.admin().indices().create(request, actionListener);
        }

        @Override
        public boolean isCreated() {
            return clusterService.state().getRoutingTable().hasIndex(MODEL_INDEX_NAME);
        }

        @Override
        public void put(String modelId, Model model, ActionListener<AcknowledgedResponse> listener) throws IOException {
            String base64Model = Base64.getEncoder().encodeToString(model.getModelBlob());

            Map<String, Object> parameters = ImmutableMap.of(
                    KNNConstants.KNN_ENGINE, model.getModelMetadata().getKnnEngine().getName(),
                    KNNConstants.METHOD_PARAMETER_SPACE_TYPE, model.getModelMetadata().getSpaceType().getValue(),
                    KNNConstants.DIMENSION, model.getModelMetadata().getDimension(),
                    KNNConstants.MODEL_BLOB_PARAMETER, base64Model
            );

            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(MODEL_INDEX_NAME, "_doc");
            indexRequestBuilder.setId(modelId);
            indexRequestBuilder.setSource(parameters);

            // Fail if the id already exists. Models are not updateable
            indexRequestBuilder.setOpType(DocWriteRequest.OpType.CREATE);
            indexRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            // After the model is indexed, update metadata
            ActionListener<IndexResponse> putMetadataListener = getUpdateModelMetadataListener(model.getModelMetadata(),
                    listener);

            if (!isCreated()) {
                create(ActionListener.wrap(createIndexResponse -> indexRequestBuilder.execute(putMetadataListener),
                        listener::onFailure));
                return;
            }

            indexRequestBuilder.execute(putMetadataListener);
        }

        @Override
        public void put(Model model, ActionListener<AcknowledgedResponse> listener) throws IOException {
            String base64Model = Base64.getEncoder().encodeToString(model.getModelBlob());

            Map<String, Object> parameters = ImmutableMap.of(
                    KNNConstants.KNN_ENGINE, model.getModelMetadata().getKnnEngine().getName(),
                    KNNConstants.METHOD_PARAMETER_SPACE_TYPE, model.getModelMetadata().getSpaceType().getValue(),
                    KNNConstants.DIMENSION, model.getModelMetadata().getDimension(),
                    KNNConstants.MODEL_BLOB_PARAMETER, base64Model
            );

            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(MODEL_INDEX_NAME, "_doc");
            indexRequestBuilder.setSource(parameters);

            // Fail if the id already exists. Models are not updateable
            indexRequestBuilder.setOpType(DocWriteRequest.OpType.CREATE);
            indexRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            // After the model is indexed, update metadata
            ActionListener<IndexResponse> putMetadataListener = getUpdateModelMetadataListener(model.getModelMetadata(),
                    listener);

            // If the index has not been created yet, create it and then add the document
            if (!isCreated()) {
                create(ActionListener.wrap(createIndexResponse -> indexRequestBuilder.execute(putMetadataListener),
                        listener::onFailure));
                return;
            }

            indexRequestBuilder.execute(putMetadataListener);
        }

        private ActionListener<IndexResponse> getUpdateModelMetadataListener(ModelMetadata modelMetadata,
                ActionListener<AcknowledgedResponse> listener) {
            return ActionListener.wrap(indexResponse -> client.execute(
                    UpdateModelMetadataAction.INSTANCE,
                    new UpdateModelMetadataRequest(indexResponse.getId(), false, modelMetadata),
                    listener
            ), listener::onFailure);
        }

        @Override
        public Model get(String modelId) throws ExecutionException, InterruptedException {
            /*
                GET /<model_index>/<modelId>?_local
            */
            GetRequestBuilder getRequestBuilder = new GetRequestBuilder(client, GetAction.INSTANCE, MODEL_INDEX_NAME)
                    .setId(modelId)
                    .setPreference("_local");
            GetResponse getResponse = getRequestBuilder.execute().get();

            Map<String, Object> responseMap = getResponse.getSourceAsMap();
            Object engine = responseMap.get(KNNConstants.KNN_ENGINE);
            Object space = responseMap.get(KNNConstants.METHOD_PARAMETER_SPACE_TYPE);
            Object dimension = responseMap.get(KNNConstants.DIMENSION);
            Object blob = responseMap.get(KNNConstants.MODEL_BLOB_PARAMETER);

            if (blob == null) {
                throw new IllegalArgumentException("No model available in \"" + MODEL_INDEX_NAME + "\" index with id \""
                        + modelId + "\".");
            }
            ModelMetadata modelMetadata = new ModelMetadata(KNNEngine.getEngine((String) engine),
                    SpaceType.getSpace((String) space), (Integer) dimension);
            return new Model(modelMetadata, Base64.getDecoder().decode((String) blob));
        }

        @Override
        public ModelMetadata getMetadata(String modelId) {
            IndexMetadata indexMetadata = clusterService.state().metadata().index(MODEL_INDEX_NAME);

            if (indexMetadata == null) {
                throw new RuntimeException("Model index's metadata does not exist");
            }

            Map<String, String> models = indexMetadata.getCustomData(MODEL_METADATA_FIELD);
            if (models == null) {
                throw new RuntimeException("Model metadata does not exist");
            }

            String modelMetadata = models.get(modelId);

            if (modelMetadata == null) {
                throw new RuntimeException("Model \"" + modelId + "\" does not exist");
            }

            return ModelMetadata.fromString(modelMetadata);
        }

        private String getMapping() throws IOException {
            URL url = ModelDao.class.getClassLoader().getResource(MODEL_INDEX_MAPPING_PATH);
            if (url == null) {
                throw new IllegalStateException("Unable to retrieve mapping for \"" + MODEL_INDEX_NAME + "\"");
            }

            return Resources.toString(url, Charsets.UTF_8);
        }

        @Override
        public void delete(String modelId, ActionListener<DeleteResponse> listener) {
            // If the index is not created, there is no need to delete the model
            if (!isCreated()) {
                logger.info("Cannot delete model \"" + modelId + "\". Model index does not exist.");
                return;
            }

            // Setup delete model request
            DeleteRequestBuilder deleteRequestBuilder = new DeleteRequestBuilder(client, DeleteAction.INSTANCE,
                    MODEL_INDEX_NAME);
            deleteRequestBuilder.setId(modelId);
            deleteRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            // On model deletion from the index, remove the model from the model cache
            ActionListener<DeleteResponse> onModelDeleteListener = ActionListener.wrap(deleteResponse -> {
                ModelCache.getInstance().remove(modelId);
                listener.onResponse(deleteResponse);
            }, listener::onFailure);

            // On model metadata removal, delete the model from the index
            ActionListener<AcknowledgedResponse> onMetadataUpdateListener = ActionListener.wrap(acknowledgedResponse ->
                            deleteRequestBuilder.execute(onModelDeleteListener), listener::onFailure);

            // Remove the metadata asynchronously
            client.execute(
                    UpdateModelMetadataAction.INSTANCE,
                    new UpdateModelMetadataRequest(modelId,  true, null),
                    onMetadataUpdateListener
            );
        }
    }
}
