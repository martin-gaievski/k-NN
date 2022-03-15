/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.plugin;

import org.opensearch.knn.index.codec.KNN87Codec.KNN87Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.opensearch.index.codec.CodecService;
import org.opensearch.knn.index.codec.KNN90Codec.KNN90Codec;

/**
 * KNNCodecService to inject the right KNNCodec version
 */
class KNNCodecService extends CodecService {

    KNNCodecService() {
        super(null, null);
    }

    /**
     * If the index is of type KNN i.e index.knn = true, We always
     * return the KNN Codec
     *
     * @param name dummy name
     * @return Latest KNN Codec
     */
    @Override
    public Codec codec(String name) {
        //Codec codec = Codec.forName(KNN87Codec.KNN_87);
        Codec codec = Codec.forName(KNN90Codec.KNN_90);
        if (codec == null) {
            throw new IllegalArgumentException("failed to find codec [" + name + "]");
        }
        return codec;
    }

    public void setPostingsFormat(PostingsFormat postingsFormat) {
        ((KNN90Codec)codec("")).setPostingsFormat(postingsFormat);
    }
}
