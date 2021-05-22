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
    public static final String SPACE_TYPE = "spaceType"; // used as field info key
    public static final String HNSW_ALGO_M = "M";
    public static final String HNSW_ALGO_EF_CONSTRUCTION = "efConstruction";
    public static final String HNSW_ALGO_INDEX_THREAD_QTY = "indexThreadQty";
    public static final String DIMENSION = "dimension";
    public static final String KNN_ENGINE = "engine";
    public static final String KNN_METHOD= "method";
    public static final String NAME = "name";
    public static final String PARAMETERS = "parameters";

    public static final String NMSLIB_NAME = "nmslib";

    public static final String METHOD_HNSW = "hnsw";

    public static final String METHOD_PARAMETER_EF_CONSTRUCTION = "ef_construction";
    public static final String METHOD_PARAMETER_M = "m";
    public static final String METHOD_PARAMETER_SPACE_TYPE = "space_type"; // used for mapping parameter

    public static final String COMPOUND_EXTENSION = "c";
}
