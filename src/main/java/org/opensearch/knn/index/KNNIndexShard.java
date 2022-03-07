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

package org.opensearch.knn.index;

import org.apache.lucene.index.FieldInfo;
import org.opensearch.knn.common.KNNConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.knn.index.util.KNNEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensearch.knn.index.codec.KNNCodecUtil.buildEngineFileName;

/**
 * KNNIndexShard wraps IndexShard and adds methods to perform k-NN related operations against the shard
 */
public class KNNIndexShard {
    private IndexShard indexShard;
    private KNNIndexCache knnIndexCache;

    private static Logger logger = LogManager.getLogger(KNNIndexShard.class);

    /**
     * Constructor to generate KNNIndexShard. We do not perform validation that the index the shard is from
     * is in fact a k-NN Index (index.knn = true). This may make sense to add later, but for now the operations for
     * KNNIndexShards that are not from a k-NN index should be no-ops.
     *
     * @param indexShard IndexShard to be wrapped.
     */
    public KNNIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
        this.knnIndexCache = KNNIndexCache.getInstance();
    }

    /**
     * Return the underlying IndexShard
     *
     * @return IndexShard
     */
    public IndexShard getIndexShard() {
        return indexShard;
    }

    /**
     * Return the name of the shards index
     *
     * @return Name of shard's index
     */
    public String getIndexName() {
        return indexShard.shardId().getIndexName();
    }

    /**
     * Load all of the HNSW graphs for this shard into the cache. Note that getIndices is called to prevent loading
     * in duplicates.
     *
     * @throws IOException Thrown when getting the HNSW Paths to be loaded in
     */
    public void warmup() throws IOException {
        logger.info("[KNN] Warming up index: " + getIndexName());
        try (Engine.Searcher searcher = indexShard.acquireSearcher("knn-warmup")) {
            Map<String, SpaceType> allEnginePaths = getAllEnginePaths(searcher.getIndexReader());
            knnIndexCache.loadIndices(allEnginePaths, getIndexName());
        }
    }

    /**
     * For the given shard, get all of its engine paths
     *
     * @param indexReader IndexReader to read the file paths for the shard
     * @return List of engine file Paths
     * @throws IOException Thrown when the SegmentReader is attempting to read the segments files
     */
    public Map<String, SpaceType> getAllEnginePaths(IndexReader indexReader) throws IOException {
        Map<String, SpaceType> engineFiles = new HashMap<>();
        for (KNNEngine knnEngine : KNNEngine.values()) {
            engineFiles.putAll(getEnginePaths(indexReader, knnEngine));
        }
        return engineFiles;
    }

    private Map<String, SpaceType> getEnginePaths(IndexReader indexReader, KNNEngine knnEngine) throws IOException {
        Map<String, SpaceType> engineFiles = new HashMap<>();

        for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
            SegmentReader reader = (SegmentReader) FilterLeafReader.unwrap(leafReaderContext.reader());
            Path shardPath = ((FSDirectory) FilterDirectory.unwrap(reader.directory())).getDirectory();
            String fileExtension = reader.getSegmentInfo().info.getUseCompoundFile()
                    ? knnEngine.getCompoundExtension() : knnEngine.getExtension();

            for (FieldInfo fieldInfo : reader.getFieldInfos()) {
                if (fieldInfo.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD)) {
                    SpaceType spaceType = SpaceType.getSpace(fieldInfo.attributes().get(KNNConstants.SPACE_TYPE));
                    String engineFileName = buildEngineFileName(reader.getSegmentInfo().info.name,
                            knnEngine.getLatestBuildVersion(), fieldInfo.name, fileExtension);

                    engineFiles.putAll(reader.getSegmentInfo().files().stream()
                            .filter(fileName -> fileName.equals(engineFileName))
                            .map(fileName -> shardPath.resolve(fileName).toString())
                            .filter(Objects::nonNull)
                            .collect(Collectors.toMap(fileName -> fileName, fileName -> spaceType)));
                }
            }
        }
        return engineFiles;
    }
}
