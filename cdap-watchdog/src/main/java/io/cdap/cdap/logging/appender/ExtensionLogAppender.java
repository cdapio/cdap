/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.publisher.DefaultLogPublisherContext;
import io.cdap.cdap.logging.publisher.LogPublisherExtensionLoader;
import io.cdap.cdap.spi.logs.LogPublisher;
import io.cdap.cdap.spi.logs.LogPublisherContext;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link LogAppender} that delegates log messages to a dynamically loaded log publisher.
 */
public class ExtensionLogAppender extends LogAppender {

  private static final Logger LOG = LoggerFactory.getLogger(ExtensionLogAppender.class);
  private static final LogPublisher NO_OP_LOG_PUBLISHER = new NoOpLogPublisher();
  private static final String APPENDER_NAME = ExtensionLogAppender.class.getName();

  @VisibleForTesting
  final AtomicReference<LogPublisher> publisher;
  private final String provider;
  private final CConfiguration cConf;

  /**
   * Constructs an ExtensionLogAppender with the given configuration.
   *
   * @param cConf The configuration object containing the properties to configure the appender.
   */
  @Inject
  public ExtensionLogAppender(CConfiguration cConf) {
    this.cConf = cConf;
    this.provider = cConf.get(Constants.Logging.LOG_PUBLISHER_PROVIDER);
    this.publisher = new AtomicReference<>(NO_OP_LOG_PUBLISHER);
    setName(APPENDER_NAME);
  }

  /**
   * Starts the appender and initializes the log publisher.
   */
  @Override
  public void start() {
    LogPublisher newPublisher = loadLogPublisher();
    publisher.getAndSet(newPublisher).close();
    publisher.get().initialize(new DefaultLogPublisherContext(cConf, provider));
    super.start();
    LOG.info("Successfully started {}.", APPENDER_NAME);
  }

  /**
   * Stops the appender and releases resources associated with the log publisher.
   */
  @Override
  public void stop() {
    super.stop();
    publisher.getAndSet(NO_OP_LOG_PUBLISHER).close();
    LOG.info("Successfully stopped {}.", APPENDER_NAME);
  }

  /**
   * Appends a log message by delegating to the log publisher.
   *
   * @param logMessage The log message to append.
   */
  @Override
  protected void appendEvent(LogMessage logMessage) {
    logMessage.prepareForDeferredProcessing();
    logMessage.getCallerData();
    publisher.get().publish(logMessage);
  }

  /**
   * Loads the log publisher using the extension loader.
   *
   * @return the {@link LogPublisher} instance
   */
  private LogPublisher loadLogPublisher() {
    if (!cConf.getBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED)) {
      LOG.debug("Log publisher is disabled; using NoOpPublisher.");
      return NO_OP_LOG_PUBLISHER;
    }

    LogPublisher logPublisher = Optional.ofNullable(
            new LogPublisherExtensionLoader(cConf).get(provider))
        .orElseThrow(() -> new RuntimeException("Failed to load log publisher: " + provider));

    LOG.debug("Loaded log publisher: {}", logPublisher.getName());
    return logPublisher;
  }

  /**
   * A no-operation implementation of LogPublisher to handle cases where no publisher is
   * configured.
   */
  private static class NoOpLogPublisher implements LogPublisher {

    @Override
    public String getName() {
      return "NoOpPublisher";
    }

    @Override
    public void publish(ILoggingEvent event) {
      // No-op
    }

    @Override
    public void initialize(LogPublisherContext context) {
      // No-op
    }

    @Override
    public void close() {
      // No-op
    }
  }
}
