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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.util.KNNEngine;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.knn.common.KNNConstants.DIMENSION;
import static org.opensearch.knn.common.KNNConstants.KNN_ENGINE;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_SPACE_TYPE;
import static org.opensearch.knn.common.KNNConstants.MODEL_BLOB_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.MODEL_DESCRIPTION;
import static org.opensearch.knn.common.KNNConstants.MODEL_ERROR;
import static org.opensearch.knn.common.KNNConstants.MODEL_ID;
import static org.opensearch.knn.common.KNNConstants.MODEL_STATE;
import static org.opensearch.knn.common.KNNConstants.MODEL_TIMESTAMP;

public class Model implements Writeable, ToXContent {

    private String modelID;
    private ModelMetadata modelMetadata;
    private AtomicReference<byte[]> modelBlob;

    //TODO: Remove this constructor, by migrate dependents to other constructor
    /**
     * Constructor
     *
     * @param modelMetadata metadata about the model
     * @param modelBlob binary representation of model template index. Can be null if model is not yet in CREATED state.
     */
    public Model(ModelMetadata modelMetadata, @Nullable byte[] modelBlob) {
        this.modelMetadata = Objects.requireNonNull(modelMetadata, "modelMetadata must not be null");

        if (ModelState.CREATED.equals(this.modelMetadata.getState()) && modelBlob == null) {
            throw new IllegalArgumentException("Cannot construct model in state CREATED when model binary is null. " +
                    "State must be either TRAINING or FAILED");
        }

        this.modelBlob = new AtomicReference<>(modelBlob);
    }

    /**
     * Constructor
     *
     * @param modelMetadata metadata about the model
     * @param modelBlob binary representation of model template index. Can be null if model is not yet in CREATED state.
     * @param modelID model identifier
     */
    public Model(ModelMetadata modelMetadata, @Nullable byte[] modelBlob, String modelID) {
        this(modelMetadata,modelBlob);
        this.modelID = Objects.requireNonNull(modelID, "model id must not be null");
    }

    private byte[] readOptionalModelBlob(StreamInput in) throws IOException {
        return in.readBoolean() ? in.readByteArray(): null;
    }

    /**
     * Constructor
     *
     * @param in Stream input
     */
    public Model(StreamInput in) throws IOException {
        this.modelMetadata = new ModelMetadata(in);
        this.modelBlob = new AtomicReference<>(readOptionalModelBlob(in));
        this.modelID = in.readOptionalString();
    }


    /**
     * getter for model's metadata
     *
     * @return model's metadata
     */
    public ModelMetadata getModelMetadata() {
        return modelMetadata;
    }

    /**
     * getter for model's identifier
     *
     * @return model's id
     */
    public String getModelID() {
        return modelID;
    }

    /**
     * getter for model's binary blob
     *
     * @return modelBlob
     */
    public byte[] getModelBlob() {
        return modelBlob.get();
    }

    /**
     * getter for model's length
     *
     * @return length of model blob
     */
    public int getLength() {
        if (getModelBlob() == null) {
            return 0;
        }
        return getModelBlob().length;
    }

    /**
     * Sets model blob to new value
     *
     * @param modelBlob updated model blob
     */
    public synchronized void setModelBlob(byte[] modelBlob) {
        this.modelBlob = new AtomicReference<>(Objects.requireNonNull(modelBlob, "model blob cannot be updated to null"));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        Model other = (Model) obj;

        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(getModelMetadata(), other.getModelMetadata());
        equalsBuilder.append(getModelBlob(), other.getModelBlob());

        return equalsBuilder.isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getModelMetadata()).append(getModelBlob()).toHashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if(Strings.hasText(modelID)){
            builder.field(MODEL_ID, modelID);
        }
        builder.field(MODEL_STATE, getModelMetadata().getState().getName());
        builder.field(MODEL_TIMESTAMP, getModelMetadata().getTimestamp());
        builder.field(MODEL_DESCRIPTION, getModelMetadata().getDescription());
        builder.field(MODEL_ERROR, getModelMetadata().getError());

        String base64Model = "";
        if(getModelBlob() != null){
            base64Model = Base64.getEncoder().encodeToString(getModelBlob());
        }
        builder.field(MODEL_BLOB_PARAMETER, base64Model);

        builder.field(METHOD_PARAMETER_SPACE_TYPE, getModelMetadata().getSpaceType().getValue());
        builder.field(DIMENSION, getModelMetadata().getDimension());
        builder.field(KNN_ENGINE, getModelMetadata().getKnnEngine().getName());

        return xContentBuilder.endObject();
    }

    private void writeOptionalModelBlob(StreamOutput output) throws IOException {
        if(getModelBlob() == null){
            output.writeBoolean(false);
            return;
        }
        output.writeBoolean(true);
        output.writeByteArray(getModelBlob());
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param output instance of {@linkplain StreamOutput}.
     */
    @Override
    public void writeTo(StreamOutput output) throws IOException {
        getModelMetadata().writeTo(output);
        writeOptionalModelBlob(output);
        output.writeOptionalString(modelID);
    }
}
