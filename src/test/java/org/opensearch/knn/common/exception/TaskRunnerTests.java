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

package org.opensearch.knn.common.exception;

import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.common.TaskRunner;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.function.Supplier;

public class TaskRunnerTests extends KNNTestCase {

    public void testRunWithStashedContextRunnable() {
        ThreadPool threadPool = new TestThreadPool(this.getClass().getSimpleName() + "ThreadPool");
        threadPool.getThreadContext().putHeader("key", "value");
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool);

        assertTrue(client.threadPool().getThreadContext().getHeaders().containsKey("key"));

        Runnable runnable = () -> {
            assertFalse(client.threadPool().getThreadContext().getHeaders().containsKey("key"));
        };
        TaskRunner.runWithStashedThreadContext(client, () -> runnable);

        assertTrue(client.threadPool().getThreadContext().getHeaders().containsKey("key"));

        threadPool.shutdownNow();
        client.close();
    }

    public void testRunWithStashedContextSupplier() {
        ThreadPool threadPool = new TestThreadPool(this.getClass().getSimpleName() + "ThreadPool");
        threadPool.getThreadContext().putHeader("key", "value");
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool);

        assertTrue(client.threadPool().getThreadContext().getHeaders().containsKey("key"));

        Supplier<String> supplier = () -> {
            assertFalse(client.threadPool().getThreadContext().getHeaders().containsKey("key"));
            return this.getClass().getName();
        };
        TaskRunner.runWithStashedThreadContext(client, () -> supplier);

        assertTrue(client.threadPool().getThreadContext().getHeaders().containsKey("key"));

        threadPool.shutdownNow();
        client.close();
    }
}
