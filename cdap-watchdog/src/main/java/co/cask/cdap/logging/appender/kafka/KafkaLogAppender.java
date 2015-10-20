/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.logging.appender.kafka;

import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.search.run.LogSearchDocument;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Log appender that publishes log messages to Kafka.
 */
public final class KafkaLogAppender extends LogAppender {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaLogAppender.class);
  private final Supplier<EndpointStrategy> endpointStrategySupplier;

  public static final String APPENDER_NAME = "KafkaLogAppender";
  private final SimpleKafkaProducer producer;
  private final LoggingEventSerializer loggingEventSerializer;

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  @Inject
  public KafkaLogAppender(CConfiguration configuration, final DiscoveryServiceClient discoveryClient) {
    setName(APPENDER_NAME);
    addInfo("Initializing KafkaLogAppender...");

    this.producer = new SimpleKafkaProducer(configuration);
    try {
      this.loggingEventSerializer = new LoggingEventSerializer();
    } catch (IOException e) {
      addError("Error initializing KafkaLogAppender.", e);
      throw Throwables.propagate(e);
    }
    addInfo("Successfully initialized KafkaLogAppender.");

    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.SEARCH));
      }
    });
  }

  private URL resolve(String resource) throws IOException {
    InetSocketAddress addr = getSearchServiceAddress();
    String url = String.format("http://%s:%d%s/%s", addr.getHostName(), addr.getPort(),
                               Constants.Gateway.API_VERSION_3, resource);
    return new URL(url);
  }

  @Override
  protected void append(LogMessage logMessage) {
    try {

      LogSearchDocument logSearchDocument = new LogSearchDocument("log", logMessage.getLevel().toString().toLowerCase(),
                                                                  logMessage.getTimeStamp(),
                                                                  logMessage.getFormattedMessage(),
                                                                  logMessage.getLoggerName(),
                                                                  logMessage.getThrowableProxy() == null ? null
                                                                    : logMessage.getThrowableProxy().getClassName(),
                                                                  logMessage.getThrowableProxy() == null ? null
                                                                    : logMessage.getThrowableProxy()
                                                                    .getStackTraceElementProxyArray());

      HttpRequest.Builder requestBuilder = HttpRequest.get(resolve("search/indexes/log"));
      HttpRequests.execute(requestBuilder.withBody(new Gson().toJson(logSearchDocument)).build());

      byte[] bytes = loggingEventSerializer.toBytes(logMessage.getLoggingEvent(), logMessage.getLoggingContext());
      producer.publish(logMessage.getLoggingContext().getLogPartition(), bytes);
    } catch (Throwable t) {
      LOG.error("Got exception while serializing log event {}.", logMessage.getLoggingEvent(), t);
    }
  }

  @Override
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      return;
    }

    super.stop();
    producer.stop();
  }

  private InetSocketAddress getSearchServiceAddress() {
    Discoverable discoverable = endpointStrategySupplier.get().pick(3L, TimeUnit.SECONDS);
    if (discoverable != null) {
      return discoverable.getSocketAddress();
    }
    throw new ServiceUnavailableException(Constants.Service.SEARCH);
  }
}
