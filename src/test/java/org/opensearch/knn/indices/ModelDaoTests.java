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

import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.knn.KNNSingleNodeTestCase;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.util.KNNEngine;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.knn.common.KNNConstants.DIMENSION;
import static org.opensearch.knn.common.KNNConstants.KNN_ENGINE;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_SPACE_TYPE;
import static org.opensearch.knn.common.KNNConstants.MODEL_BLOB_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.MODEL_INDEX_NAME;

public class ModelDaoTests extends KNNSingleNodeTestCase {

    public void testCreate() throws IOException, InterruptedException {
        int attempts = 3;
        final CountDownLatch inProgressLatch = new CountDownLatch(attempts);

        ActionListener<CreateIndexResponse> indexCreationListener = ActionListener.wrap(response -> {
            assertTrue(response.isAcknowledged());
            inProgressLatch.countDown();
        }, exception -> {
            if (!(ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException)) {
                fail("Failed for reason other than ResourceAlreadyExistsException: " + exception);
            }
            inProgressLatch.countDown();
        });

        ModelDao modelDao = ModelDao.OpenSearchKNNModelDao.getInstance();

        for (int i = 0; i < attempts; i++) {
            modelDao.create(indexCreationListener);
        }

        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testExists() {
        ModelDao modelDao = ModelDao.OpenSearchKNNModelDao.getInstance();
        assertFalse(modelDao.isCreated());
        createIndex(MODEL_INDEX_NAME);
        assertTrue(modelDao.isCreated());
    }

    public void testPut_withId() throws InterruptedException, IOException {
        createIndex(MODEL_INDEX_NAME);

        ModelDao modelDao = ModelDao.OpenSearchKNNModelDao.getInstance();
        String modelId = "efbsdhcvbsd"; // User provided model id
        byte [] modelBlob = "hello".getBytes();
        int dimension = 2;

        // Listener to confirm that everything was updated as expected
        final CountDownLatch inProgressLatch1 = new CountDownLatch(1);
        ActionListener<AcknowledgedResponse> docCreationListener = ActionListener.wrap(response -> {
            assertTrue(response.isAcknowledged());
            inProgressLatch1.countDown();
        }, exception -> fail("Unable to put the model: " + exception));

        Model model = new Model(new ModelMetadata(KNNEngine.DEFAULT, SpaceType.DEFAULT, dimension), modelBlob);
        modelDao.put(modelId, model, docCreationListener);

        assertTrue(inProgressLatch1.await(100, TimeUnit.SECONDS));

        // User provided model id that already exists
        final CountDownLatch inProgressLatch2 = new CountDownLatch(1);
        ActionListener<AcknowledgedResponse> docCreationListenerDuplicateId = ActionListener.wrap(
                response -> fail("Model already exists, but creation was successful"),
                exception -> {
                    if (!(ExceptionsHelper.unwrapCause(exception) instanceof VersionConflictEngineException)) {
                        fail("Unable to put the model: " + exception);
                    }
                    inProgressLatch2.countDown();
        });

        modelDao.put(modelId, model, docCreationListenerDuplicateId);
        assertTrue(inProgressLatch2.await(100, TimeUnit.SECONDS));
    }

    public void testPut_withoutId() throws InterruptedException, IOException {
        ModelDao modelDao = ModelDao.OpenSearchKNNModelDao.getInstance();
        byte [] modelBlob = "hello".getBytes();
        int dimension = 2;

        createIndex(MODEL_INDEX_NAME);

        // User does not provide model id
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        ActionListener<AcknowledgedResponse> docCreationListenerNoModelId = ActionListener.wrap(response -> {
                    assertTrue(response.isAcknowledged());
                    inProgressLatch.countDown();
                },
                exception -> fail("Unable to put the model: " + exception));

        Model model = new Model(new ModelMetadata(KNNEngine.DEFAULT, SpaceType.DEFAULT, dimension), modelBlob);
        modelDao.put(model, docCreationListenerNoModelId);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testGet() throws IOException, InterruptedException, ExecutionException {
        ModelDao modelDao = ModelDao.OpenSearchKNNModelDao.getInstance();
        String modelId = "efbsdhcvbsd";
        byte[] modelBlob = "hello".getBytes();
        int dimension = 2;

        // model index doesnt exist
        expectThrows(ExecutionException.class, () -> modelDao.get(modelId));

        // model id doesnt exist
        createIndex(MODEL_INDEX_NAME);
        expectThrows(Exception.class, () -> modelDao.get(modelId));

        // model id exists
        Model model = new Model(new ModelMetadata(KNNEngine.DEFAULT, SpaceType.DEFAULT, dimension), modelBlob);
        addDoc(modelId, model);
        assertArrayEquals(modelBlob, modelDao.get(modelId).getModelBlob());
    }

    public void testGetMetadata() throws IOException, InterruptedException {
        ModelDao modelDao = ModelDao.OpenSearchKNNModelDao.getInstance();

        String modelId = "test-model";

        // Model Index does not exist
        expectThrows(RuntimeException.class, () -> modelDao.getMetadata(modelId));

        createIndex(MODEL_INDEX_NAME);

        // Model id does not exist
        expectThrows(RuntimeException.class, () -> modelDao.getMetadata(modelId));

        // Model exists
        byte [] modelBlob = "hello".getBytes();

        KNNEngine knnEngine = KNNEngine.FAISS;
        SpaceType spaceType = SpaceType.INNER_PRODUCT;
        int dimension = 2;
        ModelMetadata modelMetadata = new ModelMetadata(knnEngine, spaceType, dimension);

        Model model = new Model(modelMetadata, modelBlob);

        // Listener to confirm that everything was updated as expected
        final CountDownLatch inProgressLatch1 = new CountDownLatch(1);
        ActionListener<AcknowledgedResponse> docCreationListener = ActionListener.wrap(response -> {
            assertTrue(response.isAcknowledged());

            ModelMetadata modelMetadata1 = modelDao.getMetadata(modelId);
            assertEquals(modelMetadata, modelMetadata1);

            inProgressLatch1.countDown();
        }, exception -> fail("Unable to put the model: " + exception));

        // We use put so that we can confirm cluster metadata gets added
        modelDao.put(modelId, model, docCreationListener);

        assertTrue(inProgressLatch1.await(100, TimeUnit.SECONDS));
    }

    public void testDelete() throws IOException, InterruptedException {
        ModelDao modelDao = ModelDao.OpenSearchKNNModelDao.getInstance();
        String modelId = "efbsdhcvbsd-2";
        byte[] modelBlob = "hello".getBytes();
        int dimension = 2;

        // model index doesnt exist --> nothing should happen
        modelDao.delete(modelId, null);

        // model id doesnt exist
        createIndex(MODEL_INDEX_NAME);

        final CountDownLatch inProgressLatch1 = new CountDownLatch(1);
        ActionListener<DeleteResponse> deleteModelDoesNotExistListener = ActionListener.wrap(response -> {
            assertEquals(RestStatus.NOT_FOUND, response.status());
            inProgressLatch1.countDown();
        }, exception -> fail("Unable to delete the model: " + exception));

        modelDao.delete(modelId, deleteModelDoesNotExistListener);
        assertTrue(inProgressLatch1.await(100, TimeUnit.SECONDS));

        final CountDownLatch inProgressLatch2 = new CountDownLatch(1);
        ActionListener<DeleteResponse> deleteModelExistsListener = ActionListener.wrap(response -> {
            assertEquals(modelId, response.getId());
            inProgressLatch2.countDown();
        }, exception -> fail("Unable to delete model: " + exception));

        // model id exists
        Model model = new Model(new ModelMetadata(KNNEngine.DEFAULT, SpaceType.DEFAULT, dimension), modelBlob);

        ActionListener<AcknowledgedResponse> docCreationListener = ActionListener.wrap(response -> {
            assertTrue(response.isAcknowledged());
            modelDao.delete(modelId, deleteModelExistsListener);
        }, exception -> fail("Unable to put the model: " + exception));

        // We use put so that we can confirm cluster metadata gets added
        modelDao.put(modelId, model, docCreationListener);

        assertTrue(inProgressLatch2.await(100, TimeUnit.SECONDS));
    }

    public void addDoc(String modelId, Model model) throws IOException, ExecutionException, InterruptedException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field(KNN_ENGINE, model.getModelMetadata().getKnnEngine().getName())
                .field(METHOD_PARAMETER_SPACE_TYPE, model.getModelMetadata().getSpaceType().getValue())
                .field(DIMENSION, model.getModelMetadata().getDimension())
                .field(MODEL_BLOB_PARAMETER, Base64.getEncoder().encodeToString(model.getModelBlob()))
                .endObject();
        IndexRequest indexRequest = new IndexRequest()
                .index(MODEL_INDEX_NAME)
                .id(modelId)
                .source(builder)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        IndexResponse response = client().index(indexRequest).get();
        assertEquals(response.status(), RestStatus.CREATED);
    }
}
