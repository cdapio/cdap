/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap;

import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TMSPerfService extends AbstractExecutionThreadService {
    private static final Logger LOG = LoggerFactory.getLogger(TMSPerfService.class);

    private final MessagingService messagingService;
    private final ScheduledExecutorService executor;
    private final int testDurationMillis;
    private final int payloadsPerPublish;
    private final int writesPerIteration;
//    private final int numThreads;

    public TMSPerfService(CConfiguration cConf, MessagingService messagingService) {
        this.messagingService = messagingService;
//        this.numThreads = cConf.getInt("tms.publish.test.num.threads", 20);
        this.testDurationMillis = cConf.getInt("tms.publish.test.duration.millis", 10 * 1000);
        this.payloadsPerPublish = cConf.getInt("tms.publish.test.publish.batch.size", 1);
        this.writesPerIteration = cConf.getInt("tms.publish.test.publish.writes.per.iteration", 100);
        this.executor = Executors.newScheduledThreadPool(1,
                Threads.createDaemonThreadFactory("tms-publish"));
    }

    /**
     * Creates the given topic if it is not yet created.
     * Copied from CoreMessagingService and modified.
     */
    private void createTopicIfNotExists(MessagingService messagingService, TopicId topicId) throws IOException {
        try {
            messagingService.createTopic(new TopicMetadata(topicId));
        } catch (TopicAlreadyExistsException e) {
        }
    }

    @Override
    protected void run() {
        StringBuilder sb = new StringBuilder(4000);
        for (int i = 0; i < 400; i++) {
            sb.append("0123456789");
        }
        String payload = sb.toString();
        System.out.println("payload size: " + payload.length());
        runIter(1, payload);
        runIter(5, payload);
        runIter(10, payload);
        runIter(20, payload);
        runIter(50, payload);
        runIter(100, payload);
        runIter(200, payload);
        runIter(400, payload);
    }

    public void runIter(int numThreads, String payload) {
        try {
            long startTime = System.currentTimeMillis();
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            LOG.info("Starting the TMS publish perf test.");

            List<Callable<Long>> callables = new ArrayList<>();
            List<TopicId> topicIds = new ArrayList<>();

            // give each thread its own topic
            int numTopics = numThreads;
            for (int i = 0; i < numTopics; i++) {
                topicIds.add(new TopicId("default", "topic" + i));
            }
            int threadCount = 0;
            for (TopicId topicId : topicIds) {
                createTopicIfNotExists(messagingService, topicId);
                callables.add(new TMSPublishCallable("thread" + threadCount++, topicId, messagingService, payload));
            }
            List<Future<Long>> futures = executor.invokeAll(callables, 20, TimeUnit.MINUTES);

            long totalPublished = 0;
            for (Future<Long> future : futures) {
                if (future.isCancelled()) {
                    LOG.error("Expected futures to complete within duration");
                    continue;
                }
                totalPublished += future.get();
            }
            executor.shutdownNow();

            long duration = System.currentTimeMillis() - startTime;
            LOG.info("Stopped the TMS publish test in {} milliseconds.\nThreads: {}\nPublished {} times.\n" +
                            "Average Rate: {}/sec",
                    duration, numThreads, totalPublished, totalPublished / (duration / 1000));
        } catch (Exception e) {
            LOG.error("Got error.", e);
        }
    }

    @Override
    protected ScheduledExecutorService executor() {
        return executor;
    }

    private class TMSPublishCallable implements Callable<Long> {
        private final String name;
        private final MessagingService messagingService;
        private final TopicId topicId;
        private final String payload;
        private long count;

        TMSPublishCallable(String name, TopicId topicId, MessagingService messagingService, String payload) {
            this.name = name;
            this.topicId = topicId;
            this.messagingService = messagingService;
            this.payload = payload;
        }

        @Override
        public Long call() throws TopicNotFoundException, IOException {
            try {
//                LOG.info("Starting to publish on topic {} for thread {}", topicId, name);
                Stopwatch stopwatch = new Stopwatch().start();
                while (!Thread.currentThread().isInterrupted() && stopwatch.elapsedMillis() < testDurationMillis) {
                    for (int i = 0; i < writesPerIteration; i++) {
                        StoreRequestBuilder builder = StoreRequestBuilder.of(topicId);
                        for (int j = 0; j < payloadsPerPublish; j++) {
                            builder.addPayload(payload);
                        }
                        messagingService.publish(builder.build());
                    }
                    count += writesPerIteration;
                }

//                LOG.info("[{}] To publish {} times, it took: {}", name, count, stopwatch.elapsedMillis());
                return count;
            } catch (Exception e) {
                // TODO: use ExceptionHandler instead
                LOG.error("Got error.", e);
                throw e;
            }
        }
    }

}
