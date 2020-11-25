/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;

public class KNNScoringUtil {
    private static Logger logger = LogManager.getLogger(KNNScoringUtil.class);

    /**
     * This method calculates L2 squared distance between query vector
     * and input vector
     *
     * @param queryVector query vector
     * @param inputVector input vector
     * @return L2 score
     */
    public static float l2Squared(float[] queryVector, float[] inputVector) {
        float squaredDistance = 0;
        for (int i = 0; i < inputVector.length; i++) {
            float diff = queryVector[i]-inputVector[i];
            squaredDistance += diff * diff;
        }
        return squaredDistance;
    }

    /**
     * This method can be used script to avoid repeated calculation of normalization
     * for query vector for each filtered documents
     *
     * @param queryVector query vector
     * @param inputVector input vector
     * @param normQueryVector normalized query vector value.
     * @return cosine score
     */
    public static float cosinesimilOptimized(float[] queryVector, float[] inputVector, float normQueryVector) {
        float dotProduct = 0.0f;
        float normInputVector = 0.0f;
        for (int i = 0; i < queryVector.length; i++) {
            dotProduct += queryVector[i] * inputVector[i];
            normInputVector += inputVector[i] * inputVector[i];
        }
        float normalizedProduct = normQueryVector * normInputVector;
        if (normalizedProduct == 0) {
            logger.debug("Invalid vectors for cosine. Returning minimum score to put this result to end");
            return Float.MIN_VALUE;
        }
        return (float) (dotProduct / (Math.sqrt(normalizedProduct)));
    }

    /**
     * This method calculates cosine similarity
     *
     * @param queryVector query vector
     * @param inputVector input vector
     * @return cosine score
     */
    public static float cosinesimil(float[] queryVector, float[] inputVector) {
        float dotProduct = 0.0f;
        float normQueryVector = 0.0f;
        float normInputVector = 0.0f;
        for (int i = 0; i < queryVector.length; i++) {
            dotProduct += queryVector[i] * inputVector[i];
            normQueryVector += queryVector[i] * queryVector[i];
            normInputVector += inputVector[i] * inputVector[i];
        }
        float normalizedProduct = normQueryVector * normInputVector;
        if (normalizedProduct == 0 ) {
            logger.debug("Invalid vectors for cosine. Returning minimum score to put this result to end");
            return Float.MIN_VALUE;
        }
        return (float) (dotProduct / (Math.sqrt(normalizedProduct)));
    }


    /**
     * This method calculates hamming distance on 2 BigIntegers
     *
     * @param queryBigInteger BigInteger
     * @param inputBigInteger input BigInteger
     * @return hamming distance
     */
    public static float calculateHammingBit(BigInteger queryBigInteger, BigInteger inputBigInteger) {
        return inputBigInteger.xor(queryBigInteger).bitCount();
    }

    /**
     * This method calculates hamming distance on 2 longs
     *
     * @param queryLong query Long
     * @param inputLong input Long
     * @return hamming distance
     */
    public static float calculateHammingBit(Long queryLong, Long inputLong) {
        return Long.bitCount(queryLong ^ inputLong);
    }
}
