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

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.messaging.MessageFetcher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.messaging.data.RawMessage;
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
    private final int messagesFetchedPerIteration;
//    private final int numThreads;

    public TMSPerfService(CConfiguration cConf, MessagingService messagingService) {
        this.messagingService = messagingService;
//        this.numThreads = cConf.getInt("tms.publish.test.num.threads", 20);
        this.testDurationMillis = cConf.getInt("tms.publish.test.duration.millis", 10 * 1000);
        this.payloadsPerPublish = cConf.getInt("tms.publish.test.publish.batch.size", 1);
        this.writesPerIteration = cConf.getInt("tms.publish.test.publish.writes.per.iteration", 100);

        this.messagesFetchedPerIteration = cConf.getInt("tms.fetch.test.limit.per.iteration", 100);
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
        for (int numThreads : new int[] { 1, 5, 10, 20, 50, 100, 200, 400 }) {
            runPublishTest(numThreads, payload);
        }

        for (int numThreads : new int[] { 1, 5, 10, 20, 50, 100, 200, 400 }) {
            runFetchTest(numThreads);
        }
    }

    public void runPublishTest(int numThreads, String payload) {
        try {
            long startTime = System.currentTimeMillis();
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            System.out.println("Starting the TMS publish perf test.");

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
                    System.out.println("Expected futures to complete within duration");
                    continue;
                }
                totalPublished += future.get();
            }
            executor.shutdownNow();

            long duration = System.currentTimeMillis() - startTime;
            System.out.println(String.format("Stopped the TMS publish test in %s milliseconds.\n" +
                            "Threads: %s\nPublished %s times.\n" +
                            "Average Rate: %s/sec",
                    duration, numThreads, totalPublished, 1000 * totalPublished / duration));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runFetchTest(int numThreads) {
        try {
            long startTime = System.currentTimeMillis();
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            System.out.println("Starting the TMS fetch perf test.");

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
                callables.add(new TMSSubscribeCallable("thread" + threadCount++, topicId, messagingService));
            }
            List<Future<Long>> futures = executor.invokeAll(callables, 20, TimeUnit.MINUTES);

            long totalPublished = 0;
            for (Future<Long> future : futures) {
                if (future.isCancelled()) {
                    System.out.println("Expected futures to complete within duration");
                    continue;
                }
                totalPublished += future.get();
            }
            executor.shutdownNow();

            long duration = System.currentTimeMillis() - startTime;
            System.out.println(String.format("Stopped the TMS fetch test in %s milliseconds.\nThreads: %s\n" +
                            "Fetched %s times.\n" +
                            "Average Rate: %s/sec",
                    duration, numThreads, totalPublished, 1000 * totalPublished / duration));
        } catch (Exception e) {
            e.printStackTrace();
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

    private class TMSSubscribeCallable implements Callable<Long> {
        private final String name;
        private final MessagingService messagingService;
        private final TopicId topicId;
        private long count;

        TMSSubscribeCallable(String name, TopicId topicId, MessagingService messagingService) {
            this.name = name;
            this.topicId = topicId;
            this.messagingService = messagingService;
        }

        @Override
        public Long call() throws TopicNotFoundException, IOException {
            try {
//                LOG.info("Starting to publish on topic {} for thread {}", topicId, name);
                byte[] messageId = null;
                Stopwatch stopwatch = new Stopwatch().start();
                while (!Thread.currentThread().isInterrupted() && stopwatch.elapsedMillis() < testDurationMillis) {

                    for (int i = 0; i < writesPerIteration; i++) {
                        MessageFetcher messageFetcher = messagingService.prepareFetch(topicId);
                        messageFetcher.setLimit(messagesFetchedPerIteration);
                        messageFetcher.setStartMessage(messageId, false);
                        try (CloseableIterator<RawMessage> iter = messageFetcher.fetch()) {
                            if (!iter.hasNext()) {
                                // topic is completely consumed
                                return count;
                            }
                            while (iter.hasNext()) {
                                RawMessage message = iter.next();
                                messageId = message.getId();
                                count++;
                            }
                        }
                    }
                }

//                LOG.info("[{}] To publish {} times, it took: {}", name, count, stopwatch.elapsedMillis());
                return count;
            } catch (Exception e) {
                // TODO: use ExceptionHandler instead
                e.printStackTrace();
                throw e;
            }
        }
    }

}
