/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN90Codec;

import org.opensearch.knn.index.codec.KNNCodecTestCase;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class KNN90CodecTests extends KNNCodecTestCase {

    public void testFooter() throws Exception {
        testFooter(new KNN90Codec());
    }

    public void testMultiFieldsKnnIndex() throws Exception {
        testMultiFieldsKnnIndex(new KNN90Codec());
    }

    public void testBuildFromModelTemplate() throws InterruptedException, ExecutionException, IOException {
        testBuildFromModelTemplate(new KNN90Codec());
    }
}
