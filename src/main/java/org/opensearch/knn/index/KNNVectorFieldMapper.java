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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.knn.index;

import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.knn.common.KNNConstants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.Explicit;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.knn.index.util.KNNEngine;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.knn.common.KNNConstants.KNN_ENGINE;
import static org.opensearch.knn.common.KNNConstants.KNN_METHOD;
import static org.opensearch.knn.common.KNNConstants.METHOD_HNSW;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_M;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_SPACE_TYPE;
import static org.opensearch.knn.common.KNNConstants.PARAMETERS;
import static org.opensearch.knn.common.KNNConstants.SPACE_TYPE;

/**
 * Field Mapper for KNN vector type.
 *
 * Extends ParametrizedFieldMapper in order to easily configure mapping parameters.
 */
public class KNNVectorFieldMapper extends ParametrizedFieldMapper {

    private static Logger logger = LogManager.getLogger(KNNVectorFieldMapper.class);

    public static final String CONTENT_TYPE = "knn_vector";
    public static final String KNN_FIELD = "knn_field";

    /**
     * Define the max dimension a knn_vector mapping can have. This limit is somewhat arbitrary. In the future, we
     * should make this configurable.
     */
    static final int MAX_DIMENSION = 10000;

    private static KNNVectorFieldMapper toType(FieldMapper in) {
        return (KNNVectorFieldMapper) in;
    }

    /**
     * Builder for KNNVectorFieldMapper. This class defines the set of parameters that can be applied to the knn_vector
     * field type
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {
        protected Boolean ignoreMalformed;

        protected final Parameter<Boolean> stored = Parameter.boolParam("store", false,
                m -> toType(m).stored, false);
        protected final Parameter<Boolean> hasDocValues = Parameter.boolParam("doc_values", false,
                m -> toType(m).hasDocValues,  true);
        protected final Parameter<Integer> dimension = new Parameter<>(KNNConstants.DIMENSION, false,
                () -> -1,
                (n, c, o) -> {
                    if (o == null) {
                        throw new IllegalArgumentException("Dimension cannot be null");
                    }
                    int value = XContentMapValues.nodeIntegerValue(o);
                    if (value > MAX_DIMENSION) {
                        throw new IllegalArgumentException("Dimension value cannot be greater than " +
                                MAX_DIMENSION + " for vector: " + name);
                    }

                    if (value <= 0) {
                        throw new IllegalArgumentException("Dimension value must be greater than 0 " +
                                "for vector: " + name);
                    }
                    return value;
                }, m -> toType(m).dimension);

        /**
         * knnMethodContext parameter allows a user to define their k-NN library index configuration. Defaults to an L2
         * hnsw default engine index without any parameters set
         */
        protected final Parameter<KNNMethodContext> knnMethodContext = new Parameter<>(KNN_METHOD, false,
                () -> null,
                (n, c, o) -> KNNMethodContext.parse(o), m -> toType(m).knnMethod)
                .setSerializer(((b, n, v) ->{
                    b.startObject(n);
                    v.toXContent(b, ToXContent.EMPTY_PARAMS);
                    b.endObject();
                }), m -> m.getMethodComponent().getName())
                .setValidator(v -> {
                    if(v != null) v.validate();
                });

        protected final Parameter<Map<String, String>> meta = Parameter.metaParam();

        protected String spaceType;
        protected String m;
        protected String efConstruction;

        public Builder(String name) {
            super(name);
        }

        public Builder(String name, String spaceType, String m, String efConstruction) {
            super(name);
            this.spaceType = spaceType;
            this.m = m;
            this.efConstruction = efConstruction;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(stored, hasDocValues, dimension, meta, knnMethodContext);
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return KNNVectorFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        @Override
        public KNNVectorFieldMapper build(BuilderContext context) {

            // Originally, a user would use index settings to set the spaceType, efConstruction and m hnsw
            // parameters. Upon further review, it makes sense to set these parameters in the mapping of a
            // particular field. However, because users migrating from older versions will still use the index
            // settings to set these parameters, we will need to provide backwards compatibilty. In order to
            // handle this, we first check if the mapping is set, and, if so use it. If not, we fall back to
            // the parameters set in the index settings. This means that if a user sets the mappings, setting the index
            // settings will have no impact.
            KNNMethodContext methodContext = knnMethodContext.getValue();

            // Special processing here is just for nmslib
            if (methodContext == null) {
                if (this.spaceType == null) {
                    this.spaceType = getSpaceType(context.indexSettings());
                }

                if (this.m == null) {
                    this.m = getM(context.indexSettings());
                }

                if (this.efConstruction == null) {
                    this.efConstruction = getEfConstruction(context.indexSettings());
                }
            } else if (KNNEngine.NMSLIB.equals(methodContext.getEngine())) {
                if (this.spaceType == null) {
                    this.spaceType = methodContext.getSpaceType().getValue();
                }

                if (this.m == null) {
                    Map<String, Object> parameters = methodContext.getMethodComponent().getParameters();

                    if (parameters != null && parameters.containsKey(METHOD_PARAMETER_M)) {
                        this.m = parameters.get(METHOD_PARAMETER_M).toString();
                    } else {
                        this.m = KNNEngine.NMSLIB.getMethod(METHOD_HNSW).getMethodComponent().getParameters()
                                .get(METHOD_PARAMETER_M).getDefaultValue().toString();
                    }
                }

                if (this.efConstruction == null) {
                    Map<String, Object> parameters = methodContext.getMethodComponent().getParameters();

                    if (parameters != null && parameters.containsKey(METHOD_PARAMETER_EF_CONSTRUCTION)) {
                        this.efConstruction = parameters.get(METHOD_PARAMETER_EF_CONSTRUCTION).toString();
                    } else {
                        this.efConstruction = KNNEngine.NMSLIB.getMethod(METHOD_HNSW).getMethodComponent()
                                .getParameters().get(METHOD_PARAMETER_EF_CONSTRUCTION).getDefaultValue().toString();
                    }
                }
            }
            
            return new KNNVectorFieldMapper(name, new KNNVectorFieldType(buildFullName(context), meta.getValue(),
                    dimension.getValue()), multiFieldsBuilder.build(this, context),
                    ignoreMalformed(context), this.spaceType, this.m, this.efConstruction, copyTo.build(), this);
        }

        private String getSpaceType(Settings indexSettings) {
            String spaceType =  indexSettings.get(KNNSettings.INDEX_KNN_SPACE_TYPE.getKey());
            if (spaceType == null) {
                logger.info("[KNN] The setting \"" + METHOD_PARAMETER_SPACE_TYPE + "\" was not set for the index. " +
                        "Likely caused by recent version upgrade. Setting the setting to the default value="
                        + KNNSettings.INDEX_KNN_DEFAULT_SPACE_TYPE);
                return KNNSettings.INDEX_KNN_DEFAULT_SPACE_TYPE;
            }
            return spaceType;
        }

        private String getM(Settings indexSettings) {
            String m =  indexSettings.get(KNNSettings.INDEX_KNN_ALGO_PARAM_M_SETTING.getKey());
            if (m == null) {
                logger.info("[KNN] The setting \"" + KNNConstants.HNSW_ALGO_M + "\" was not set for the index. " +
                        "Likely caused by recent version upgrade. Setting the setting to the default value="
                        + KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_M);
                return String.valueOf(KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_M);
            }
            return m;
        }

        private String getEfConstruction(Settings indexSettings) {
            String efConstruction =  indexSettings.get(KNNSettings.INDEX_KNN_ALGO_PARAM_EF_CONSTRUCTION_SETTING.getKey());
            if (efConstruction == null) {
                logger.info("[KNN] The setting \"" + KNNConstants.HNSW_ALGO_EF_CONSTRUCTION + "\" was not set for" +
                        " the index. Likely caused by recent version upgrade. Setting the setting to the default value="
                        + KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION);
                return String.valueOf(KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION);
            }
            return efConstruction;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new KNNVectorFieldMapper.Builder(name);
            builder.parse(name, parserContext, node);

            if (builder.dimension.getValue() == -1) {
                throw new IllegalArgumentException("Dimension value missing for vector: " + name);
            }

            return builder;
        }
    }

    public static class KNNVectorFieldType extends MappedFieldType {

        int dimension;

        public KNNVectorFieldType(String name, Map<String, String> meta, int dimension) {
            super(name, false, false, true, TextSearchInfo.NONE, meta);
            this.dimension = dimension;
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("KNN Vector do not support fields search");
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "KNN vector do not support exact searching, use KNN queries " +
                    "instead: [" + name() + "]");
        }

        public int getDimension() {
            return dimension;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new KNNVectorIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }
    }

    protected Explicit<Boolean> ignoreMalformed;
    private final boolean stored;
    private final boolean hasDocValues;
    protected final String spaceType;
    protected final String m;
    protected final String efConstruction;
    private final Integer dimension;
    protected final KNNMethodContext knnMethod;

    public KNNVectorFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields,
                                Explicit<Boolean> ignoreMalformed, String spaceType, String m, String efConstruction,
                                CopyTo copyTo, Builder builder) {
        super(simpleName, mappedFieldType,  multiFields, copyTo);

        this.stored = builder.stored.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.dimension = builder.dimension.getValue();
        this.knnMethod = builder.knnMethodContext.getValue();
        this.ignoreMalformed = ignoreMalformed;
        this.spaceType = spaceType;
        this.m = m;
        this.efConstruction = efConstruction;

        KNNEngine knnEngine;
        if (knnMethod == null) {
            knnEngine = KNNEngine.DEFAULT;
        } else {
            knnEngine = knnMethod.getEngine();
        }

        this.fieldType = new FieldType(Defaults.FIELD_TYPE);
        this.fieldType.putAttribute(KNN_ENGINE, knnEngine.getName());

        if (KNNEngine.NMSLIB.equals(knnEngine)) {
            this.fieldType.putAttribute(SPACE_TYPE, spaceType);
            this.fieldType.putAttribute(KNNConstants.HNSW_ALGO_M, m);
            this.fieldType.putAttribute(KNNConstants.HNSW_ALGO_EF_CONSTRUCTION, efConstruction);
        } else {
            // Get the method as a map and serialize to json
            try {
                assert knnMethod != null;
                this.fieldType.putAttribute(SPACE_TYPE, knnMethod.getSpaceType().getValue());
                this.fieldType.putAttribute(PARAMETERS, Strings.toString(XContentFactory.jsonBuilder()
                        .map(knnEngine.getMethodAsMap(knnMethod))));
            } catch (IOException ioe) {
                throw new RuntimeException("Unable to create KNNVectorFieldMapper: " + ioe);
            }
        }

        this.fieldType.freeze();
    }

    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public KNNVectorFieldMapper clone() {
        return (KNNVectorFieldMapper) super.clone();
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setDocValuesType(DocValuesType.BINARY);
            FIELD_TYPE.putAttribute(KNN_FIELD, "true"); //This attribute helps to determine knn field type
            FIELD_TYPE.freeze();
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (!KNNSettings.isKNNPluginEnabled()) {
            throw new IllegalStateException("KNN plugin is disabled. To enable " +
                    "update knn.plugin.enabled setting to true");
        }

        if (KNNSettings.isCircuitBreakerTriggered()) {
            throw new IllegalStateException("Indexing knn vector fields is rejected as circuit breaker triggered." +
                    " Check _opendistro/_knn/stats for detailed state");
        }

        context.path().add(simpleName());

        ArrayList<Float> vector = new ArrayList<>();
        XContentParser.Token token = context.parser().currentToken();
        float value;
        if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            while (token != XContentParser.Token.END_ARRAY) {
                value = context.parser().floatValue();

                if (Float.isNaN(value)) {
                    throw new IllegalArgumentException("KNN vector values cannot be NaN");
                }

                if (Float.isInfinite(value)) {
                    throw new IllegalArgumentException("KNN vector values cannot be infinity");
                }

                vector.add(value);
                token = context.parser().nextToken();
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            value = context.parser().floatValue();

            if (Float.isNaN(value)) {
                throw new IllegalArgumentException("KNN vector values cannot be NaN");
            }

            if (Float.isInfinite(value)) {
                throw new IllegalArgumentException("KNN vector values cannot be infinity");
            }

            vector.add(value);
            context.parser().nextToken();
        }

        if (fieldType().dimension != vector.size()) {
            String errorMessage = String.format("Vector dimension mismatch. Expected: %d, Given: %d",
                    fieldType().dimension, vector.size());
            throw new IllegalArgumentException(errorMessage);
        }

        float[] array = new float[vector.size()];
        int i = 0;
        for (Float f : vector) {
            array[i++] = f;
        }

        VectorField point = new VectorField(name(), array, fieldType);

        context.doc().add(point);
        if (fieldType.stored()) {
            context.doc().add(new StoredField(name(), point.toString()));
        }
        context.path().remove();
    }

    @Override
    protected boolean docValuesByDefault() {
        return true;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new KNNVectorFieldMapper.Builder(simpleName(), this.spaceType, this.m, this.efConstruction).init(this);
    }

    @Override
    public final boolean parsesArrayValue() {
        return true;
    }

    @Override
    public KNNVectorFieldType fieldType() {
        return (KNNVectorFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }
    }
}
