package io.cdap.cdap.security.auth;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class MessagingAuditLogWriter  implements AuditLogWriter {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingAuditLogWriter.class);
  private static final Gson GSON = new Gson();

  private final TopicId topic;
  private final MessagingService messagingService;
  private final RetryStrategy retryStrategy;

  @Inject
  public MessagingAuditLogWriter(CConfiguration cConf, MessagingService messagingService) {
    this.topic =  NamespaceId.SYSTEM.topic(cConf.get(Constants.AuditLogging.AUDIT_LOG_EVENT_TOPIC));
    this.messagingService = messagingService;
    this.retryStrategy = RetryStrategies.exponentialDelay(10, 30, TimeUnit.MILLISECONDS);
  }

  /**
   * pushes the collection of log entry to respective messaging topic
   *
   * @param auditLogContexts
   */
  @Override
  public void publish(Queue<AuditLogContext> auditLogContexts) throws IOException {

    if (auditLogContexts.isEmpty()){
      return;
    }

    int failureCount = 0;
    long startTime = -1L;
    boolean done = false;

    while (!done) {
      try {

        //Iterate through the queue and publish
        while (!auditLogContexts.isEmpty()) {
          messagingService.publish(StoreRequestBuilder.of(topic)
                                     .addPayload(GSON.toJson(auditLogContexts.remove()))
                                     .build());
          LOG.warn("SANKET_TEST : Published audit log to TMS ");
        }
        done = true;
      } catch (IOException | AccessException e) {
        throw Throwables.propagate(e);
      } catch (TopicNotFoundException | ServiceUnavailableException e) {

        if (e instanceof TopicNotFoundException) {
          createTopicIfNeeded();
        }

        // These exceptions are retry-able due to TMS not completely started
        if (startTime < 0) {
          startTime = System.currentTimeMillis();
        }
        long retryMillis = retryStrategy.nextRetry(++failureCount, startTime);
        if (retryMillis < 0) {
          LOG.error("Failed to publish messages to TMS and exceeded retry limit.", e);
          throw Throwables.propagate(e);
        }
        LOG.debug("Failed to publish messages to TMS due to {}. Will be retried in {} ms.",
                  e.getMessage(), retryMillis);
        try {
          TimeUnit.MILLISECONDS.sleep(retryMillis);
        } catch (InterruptedException e1) {
          // Something explicitly stopping this thread. Simply just break and reset the interrupt flag.
          LOG.warn("Publishing message to TMS interrupted.");
          Thread.currentThread().interrupt();
          done = true;
        }
      }
    }

  }

  private void createTopicIfNeeded() throws IOException {
    try {
      messagingService.createTopic(new DefaultTopicMetadata(topic, Collections.emptyMap()));
      LOG.warn("Created topic {}", topic.getTopic());
    } catch (TopicAlreadyExistsException ex) {
      // no-op
    }
  }
}
