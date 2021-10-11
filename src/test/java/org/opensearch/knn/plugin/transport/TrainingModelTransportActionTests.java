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

package org.opensearch.knn.plugin.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.knn.KNNSingleNodeTestCase;
import org.opensearch.knn.index.KNNMethodContext;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.util.KNNEngine;
import org.opensearch.knn.indices.ModelDao;
import org.opensearch.knn.indices.ModelMetadata;
import org.opensearch.knn.indices.ModelState;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.knn.common.KNNConstants.KNN_ENGINE;
import static org.opensearch.knn.common.KNNConstants.METHOD_IVF;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_NLIST;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_SPACE_TYPE;
import static org.opensearch.knn.common.KNNConstants.NAME;
import static org.opensearch.knn.common.KNNConstants.PARAMETERS;
import static org.opensearch.knn.plugin.transport.TrainingJobRouterTransportAction.estimateVectorSetSizeInKb;

public class TrainingModelTransportActionTests extends KNNSingleNodeTestCase {

    public void testDoExecute() throws InterruptedException, ExecutionException, IOException {
        // Ingest training data into the cluster
        String trainingIndexName = "train-index";
        String trainingFieldName = "train-field";
        int dimension = 16;

        createIndex(trainingIndexName);
        createKnnIndexMapping(trainingIndexName, trainingFieldName, dimension);

        int trainingDataCount = 1000;
        for (int i = 0; i < trainingDataCount; i++) {
            Float[] vector = new Float[dimension];
            Arrays.fill(vector, Float.intBitsToFloat(i));
            addKnnDoc(trainingIndexName, Integer.toString(i+1), trainingFieldName, vector);
        }

        // Create train model request
        String modelId = "test-model-id";
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, METHOD_IVF)
                .field(KNN_ENGINE, KNNEngine.FAISS.getName())
                .field(METHOD_PARAMETER_SPACE_TYPE, SpaceType.INNER_PRODUCT.getValue())
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_NLIST, 4)
                .endObject()
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext = KNNMethodContext.parse(in);

        TrainingModelRequest trainingModelRequest = new TrainingModelRequest(
                modelId,
                knnMethodContext,
                dimension,
                trainingIndexName,
                trainingFieldName,
                null,
                "test-detector"
        );
        trainingModelRequest.setTrainingDataSizeInKB(estimateVectorSetSizeInKb(trainingDataCount, dimension));

        // Create listener that ensures that the test succeeds
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        ActionListener<TrainingModelResponse> listener = ActionListener.wrap(response -> {
            assertEquals(modelId, response.getModelId());
            ModelMetadata modelMetadata = ModelDao.OpenSearchKNNModelDao.getInstance().getMetadata(modelId);
            assertNotNull(modelMetadata);
            assertEquals(dimension, modelMetadata.getDimension());
            assertEquals(ModelState.CREATED, modelMetadata.getState());
            assertTrue(modelMetadata.getError() == null || modelMetadata.getError().isEmpty());
            inProgressLatch.countDown();
        }, e -> fail("Failure: " + e.getMessage()));

        TrainingModelTransportAction trainingModelTransportAction = node().injector()
                .getInstance(TrainingModelTransportAction.class);

        trainingModelTransportAction.doExecute(null, trainingModelRequest, listener);

        // Wait for timeout to confirm everything works
        assertTrue(inProgressLatch.await(50, TimeUnit.SECONDS));
    }

}
