/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index;

import com.google.common.collect.Streams;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.index.memory.NativeMemoryCacheManager;
import org.opensearch.knn.index.memory.NativeMemoryEntryContext;
import org.opensearch.knn.index.memory.NativeMemoryLoadStrategy;
import org.opensearch.knn.index.util.KNNEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.knn.common.KNNConstants.SPACE_TYPE;
import static org.opensearch.knn.index.IndexUtil.getParametersAtLoading;
import static org.opensearch.knn.index.codec.util.KNNCodecUtil.buildEngineFilePrefix;
import static org.opensearch.knn.index.codec.util.KNNCodecUtil.buildEngineFileSuffix;

/**
 * KNNIndexShard wraps IndexShard and adds methods to perform k-NN related operations against the shard
 */
public class KNNIndexShard {
    private IndexShard indexShard;
    private NativeMemoryCacheManager nativeMemoryCacheManager;

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
        this.nativeMemoryCacheManager = NativeMemoryCacheManager.getInstance();
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
     * Load all of the k-NN segments for this shard into the cache.
     *
     * @throws IOException Thrown when getting the HNSW Paths to be loaded in
     */
    public void warmup() throws IOException {
        logger.info("[KNN] Warming up index: " + getIndexName());
        try (Engine.Searcher searcher = indexShard.acquireSearcher("knn-warmup")) {
            getAllEnginePaths(searcher.getIndexReader()).forEach((key, value) -> {
                try {
                    nativeMemoryCacheManager.get(
                        new NativeMemoryEntryContext.IndexEntryContext(
                            key,
                            NativeMemoryLoadStrategy.IndexLoadStrategy.getInstance(),
                            getParametersAtLoading(value, KNNEngine.getEngineNameFromPath(key), getIndexName()),
                            getIndexName()
                        ),
                        true
                    );
                } catch (ExecutionException ex) {
                    throw new RuntimeException(ex);
                }
            });
        }

        logger.info("[KNN] Warming up files for mmap");
        final List<String> engineMMapExtensions = Arrays.stream(KNNEngine.values())
            .flatMap(engine -> engine.mmapFileExtensions().stream())
            .collect(Collectors.toList());
        final Directory dir = indexShard.store().directory();
        Arrays.stream(dir.listAll()).forEach(file -> {
            final Optional<String> maybeFileExtension = Optional.ofNullable(file)
                .filter(f -> f.contains("."))
                .map(f -> f.substring(file.lastIndexOf(".") + 1));
            if (maybeFileExtension.isPresent() == false
                || engineMMapExtensions.stream().anyMatch(extension -> extension.equals(maybeFileExtension.get())) == false) {
                return;
            }
            try {
                IndexReader reader = DirectoryReader.open(dir);
                IndexSearcher searcher = new IndexSearcher(reader);
                //Query warmUpQuery = Queries.newMatchAllQuery();
                int dim = 128, k = 100, numOfQueries = 200;

                for (int n = 0; n < numOfQueries; n++) {
                    float[] qVector = new float[dim];
                    IntStream.range(0, dim).forEach(i -> qVector[i] = 256 * new Random().nextFloat());
                    Query warmUpQuery = new KnnVectorQuery("target_field", qVector, k);
                    searcher.search(warmUpQuery, k);
                }
                logger.info(String.format("[KNN] Executed match all query for directory %s", dir.toString()));
                //final IndexInput ii = dir.openInput(file, IOContext.DEFAULT);
                //CodecUtil.checksumEntireFile(ii);
            } catch (IOException e) {
                logger.debug(String.format("[KNN] Warmup, tried to open file %s", file), e);
            }
            logger.info(String.format("[KNN] Pre-loaded file %s", file));
        });
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
        for (KNNEngine knnEngine : KNNEngine.getEnginesThatCreateCustomSegmentFiles()) {
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
                ? knnEngine.getCompoundExtension()
                : knnEngine.getExtension();

            for (FieldInfo fieldInfo : reader.getFieldInfos()) {
                if (fieldInfo.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD)) {
                    // Space Type will not be present on ES versions 7.1 and 7.4 because the only available space type
                    // was L2. So, if Space Type is not present, just fall back to L2
                    String spaceTypeName = fieldInfo.attributes().getOrDefault(SPACE_TYPE, SpaceType.L2.getValue());
                    SpaceType spaceType = SpaceType.getSpace(spaceTypeName);

                    engineFiles.putAll(
                        getEnginePaths(
                            reader.getSegmentInfo().files(),
                            reader.getSegmentInfo().info.name,
                            fieldInfo.name,
                            fileExtension,
                            shardPath,
                            spaceType
                        )
                    );
                }
            }
        }
        return engineFiles;
    }

    protected Map<String, SpaceType> getEnginePaths(
        Collection<String> files,
        String segmentName,
        String fieldName,
        String fileExtension,
        Path shardPath,
        SpaceType spaceType
    ) {
        String prefix = buildEngineFilePrefix(segmentName);
        String suffix = buildEngineFileSuffix(fieldName, fileExtension);
        return files.stream()
            .filter(fileName -> fileName.startsWith(prefix))
            .filter(fileName -> fileName.endsWith(suffix))
            .map(fileName -> shardPath.resolve(fileName).toString())
            .collect(Collectors.toMap(fileName -> fileName, fileName -> spaceType));
    }
}
