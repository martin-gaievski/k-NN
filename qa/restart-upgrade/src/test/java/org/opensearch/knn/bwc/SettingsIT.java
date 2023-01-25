/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.bwc;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexModule;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.opensearch.knn.TestUtils.NODES_BWC_CLUSTER;

public class SettingsIT extends AbstractRestartUpgradeTestCase {
    private static final String TEST_FIELD = "test-field";
    private static final int DIMENSIONS = 5;
    private static int DOC_ID = 0;
    private static final int NUM_DOCS = 10;

    // Default Legacy Field Mapping
    // space_type : "l2", engine : "nmslib", m : 16, ef_construction : 512
    public void testSettings() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()) {
            createKnnIndex(testIndex, getKNNDefaultIndexSettings(), createKnnIndexMapping(TEST_FIELD, DIMENSIONS));
            addKNNDocs(testIndex, TEST_FIELD, DIMENSIONS, DOC_ID, NUM_DOCS);
        } else {
            validateKNNIndexSettings();
        }
    }

    // KNN indexing tests when the cluster is upgraded to latest version
    public void validateKNNIndexSettings() throws Exception {
       Object mmapExtensions = getSettingByName(IndexModule.INDEX_STORE_HYBRID_MMAP_EXTENSIONS.getKey());
        assertNotNull(mmapExtensions);
        assertTrue(mmapExtensions instanceof List);
        List<String> mmapExtensionsList = (List<String>) mmapExtensions;
        assertFalse(mmapExtensionsList.isEmpty());
        deleteKNNIndex(testIndex);
    }

    Object getSettingByName(final String name) throws IOException {
        Request request = new Request("GET", "/" + testIndex + "/_settings");
        request.addParameter("include_defaults", "true");
        request.addParameter("flat_settings", "true");

        Response response = client().performRequest(request);
        try (InputStream is = response.getEntity().getContent()) {
            final Map<String, Object> settingsMap = (Map<String, Object>)((Map<String, Object>) XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true).get(testIndex)).get("settings");
            return settingsMap.get(name);
        }
    }
}
