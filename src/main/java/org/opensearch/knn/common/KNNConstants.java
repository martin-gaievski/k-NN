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
/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.knn.common;

public class KNNConstants {
    // shared across library constants
    public static final String DIMENSION = "dimension";
    public static final String KNN_ENGINE = "engine";
    public static final String KNN_METHOD= "method";
    public static final String NAME = "name";
    public static final String PARAMETERS = "parameters";
    public static final String METHOD_HNSW = "hnsw";
    public static final String METHOD_PARAMETER_EF_SEARCH = "ef_search";
    public static final String METHOD_PARAMETER_EF_CONSTRUCTION = "ef_construction";
    public static final String METHOD_PARAMETER_M = "m";
    public static final String METHOD_IVF = "ivf";
    public static final String METHOD_PARAMETER_NLIST = "nlist";
    public static final String METHOD_PARAMETER_SPACE_TYPE = "space_type"; // used for mapping parameter
    public static final String COMPOUND_EXTENSION = "c";
    public static final String JNI_LIBRARY_NAME = "OpensearchKNN";
    public static final String MODEL = "model";
    public static final String MODEL_ID = "model_id";
    public static final String MODEL_BLOB_PARAMETER = "model_blob";
    public static final String MODEL_INDEX_MAPPING_PATH = "mappings/model-index.json";
    public static final String MODEL_INDEX_NAME = ".opensearch-knn-models";
    public static final String PLUGIN_NAME = "knn";
    public static final String MODEL_METADATA_FIELD = "knn-models";

    public static final String MODEL_STATE = "state";
    public static final String MODEL_TIMESTAMP = "timestamp";
    public static final String MODEL_DESCRIPTION = "description";
    public static final String MODEL_ERROR = "error";

    public static final String KNN_THREAD_POOL_PREFIX = "knn";
    public static final String TRAIN_THREAD_POOL = "training";

    public static final String TRAINING_JOB_COUNT_FIELD_NAME = "training_job_count";
    public static final String NODES_KEY = "nodes";

    // nmslib specific constants
    public static final String NMSLIB_NAME = "nmslib";
    public static final String SPACE_TYPE = "spaceType"; // used as field info key
    public static final String HNSW_ALGO_M = "M";
    public static final String HNSW_ALGO_EF_CONSTRUCTION = "efConstruction";
    public static final String HNSW_ALGO_EF_SEARCH = "efSearch";
    public static final String HNSW_ALGO_INDEX_THREAD_QTY = "indexThreadQty";

    // Faiss specific constants
    public static final String FAISS_NAME = "faiss";
    public final static String FAISS_EXTENSION = ".faiss";
    public static final String INDEX_DESCRIPTION_PARAMETER = "index_description";
    public static final String METHOD_ENCODER_PARAMETER = "encoder";
    public static final String METHOD_PARAMETER_NPROBES = "nprobes";
    public static final String ENCODER_FLAT = "flat";
    public static final String ENCODER_PQ = "pq";
    public static final String ENCODER_PARAMETER_PQ_CODE_COUNT = "code_count";
    public static final String ENCODER_PARAMETER_PQ_CODE_SIZE = "code_size";
    public static final String FAISS_HNSW_DESCRIPTION = "HNSW";
    public static final String FAISS_IVF_DESCRIPTION = "IVF";
    public static final String FAISS_FLAT_DESCRIPTION = "Flat";
    public static final String FAISS_PQ_DESCRIPTION = "PQ";

    // Parameter defaults/limits
    public static final Integer ENCODER_PARAMETER_PQ_CODE_COUNT_DEFAULT = 1;
    public static final Integer ENCODER_PARAMETER_PQ_CODE_COUNT_LIMIT = 1024;
    public static final Integer ENCODER_PARAMETER_PQ_CODE_SIZE_DEFAULT = 8;
    public static final Integer ENCODER_PARAMETER_PQ_CODE_SIZE_LIMIT = 128;
    public static final Integer METHOD_PARAMETER_NLIST_DEFAULT = 4;
    public static final Integer METHOD_PARAMETER_NLIST_LIMIT = 20000;
}
