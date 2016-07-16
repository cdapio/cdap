/*
 * Copyright © 2016 Cask Data, Inc.
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

package org.apache.twill.internal;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.api.logging.LogThrowable;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.internal.json.LogEntryDecoder;
import org.apache.twill.internal.json.LogThrowableCodec;
import org.apache.twill.internal.json.StackTraceElementCodec;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.state.SystemMessages;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This is copied from Twill except with a hack to workaround TWILL-177,
 * since CDAP does not use the discoverService method on TwillController.
 * TODO:(CDAP-6504) remove once TWILL-177 is fixed.
 */
public abstract class AbstractTwillController extends AbstractZKServiceController implements TwillController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTwillController.class);
  private static final Gson GSON = new Gson();

  private final Queue<LogHandler> logHandlers;
  private final KafkaClientService kafkaClient;
  private DiscoveryServiceClient discoveryServiceClient;
  private volatile Cancellable logCancellable;

  public AbstractTwillController(RunId runId, ZKClient zkClient, Iterable<LogHandler> logHandlers) {
    super(runId, zkClient);
    this.logHandlers = new ConcurrentLinkedQueue<>();
    this.kafkaClient = new ZKKafkaClientService(ZKClients.namespace(zkClient, "/" + runId.getId() + "/kafka"));
    Iterables.addAll(this.logHandlers, logHandlers);
  }

  @Override
  protected synchronized void doStartUp() {
    if (!logHandlers.isEmpty()) {
      kafkaClient.startAndWait();
      logCancellable = kafkaClient.getConsumer().prepare()
        .addFromBeginning(Constants.LOG_TOPIC, 0)
        .consume(new LogMessageCallback(logHandlers));
    }
  }

  @Override
  protected void doShutDown() {
    if (logCancellable != null) {
      logCancellable.cancel();
    }
    // Safe to call stop no matter when state the KafkaClientService is in.
    kafkaClient.stopAndWait();
  }

  @Override
  public final synchronized void addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
    if (logHandlers.size() == 1) {
      kafkaClient.startAndWait();
      logCancellable = kafkaClient.getConsumer().prepare()
        .addFromBeginning(Constants.LOG_TOPIC, 0)
        .consume(new LogMessageCallback(logHandlers));
    }
  }

  @Override
  public final ServiceDiscovered discoverService(String serviceName) {
    // HACK to work around TWILL-177. CDAP never calls this method.
    if (discoveryServiceClient == null) {
      this.discoveryServiceClient = new ZKDiscoveryService(zkClient);
    }
    return discoveryServiceClient.discover(serviceName);
  }

  @Override
  public final ListenableFuture<Integer> changeInstances(String runnable, int newCount) {
    return sendMessage(SystemMessages.setInstances(runnable, newCount), newCount);
  }

  @Override
  public final ListenableFuture<String> restartAllInstances(String runnableName) {
    Command updateStateCommand = Command.Builder.of(Constants.RESTART_ALL_RUNNABLE_INSTANCES).
      build();
    Message message = SystemMessages.updateRunnableInstances(updateStateCommand, runnableName);
    return sendMessage(message, updateStateCommand.getCommand());
  }

  @Override
  public final ListenableFuture<Set<String>> restartInstances(Map<String,
    ? extends Set<Integer>> runnableToInstanceIds) {
    Map<String, String> runnableToStringInstanceIds =
      Maps.transformEntries(runnableToInstanceIds, new Maps.EntryTransformer<String, Set<Integer>, String>() {
        @Override
        public String transformEntry(String runnableName, Set<Integer> instanceIds) {
          validateInstanceIds(runnableName, instanceIds);
          return GSON.toJson(instanceIds, new TypeToken<Set<Integer>>() {
          }.getType());
        }
      });
    Command updateStateCommand = Command.Builder.of(Constants.RESTART_RUNNABLES_INSTANCES)
      .addOptions(runnableToStringInstanceIds)
      .build();
    Message message = SystemMessages.updateRunnablesInstances(updateStateCommand);

    return sendMessage(message, runnableToInstanceIds.keySet());
  }

  @Override
  public ListenableFuture<String> restartInstances(final String runnable, int instanceId, int... moreInstanceIds) {
    Set<Integer> instanceIds = Sets.newLinkedHashSet();
    instanceIds.add(instanceId);
    for (int id : moreInstanceIds) {
      instanceIds.add(id);
    }

    return Futures.transform(restartInstances(ImmutableMap.of(runnable, instanceIds)),
                             new Function<Set<String>, String>() {
                               public String apply(Set<String> input) {
                                 return runnable;
                               }
                             });
  }

  private void validateInstanceIds(String runnable, Set<Integer> instanceIds) {
    ResourceReport resourceReport = getResourceReport();
    if (resourceReport == null) {
      throw new IllegalStateException("Unable to get resource report since application has not started.");
    }
    Collection<TwillRunResources> runnableResources = resourceReport.getRunnableResources(runnable);
    if (runnableResources == null) {
      throw new RuntimeException("Unable to verify run resources for runnable " + runnable);
    }
    Set<Integer> existingInstanceIds = Sets.newHashSet();
    for (TwillRunResources twillRunResources : runnableResources) {
      existingInstanceIds.add(twillRunResources.getInstanceId());
    }
    LOG.info("Existing instance ids: {}", existingInstanceIds);
    for (int instanceId : instanceIds) {
      if (!existingInstanceIds.contains(instanceId)) {
        throw new IllegalArgumentException("Unable to find instance id " + instanceId + " for " + runnable);
      }
    }
  }

  private static final class LogMessageCallback implements KafkaConsumer.MessageCallback {

    private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(LogEntry.class, new LogEntryDecoder())
      .registerTypeAdapter(LogThrowable.class, new LogThrowableCodec())
      .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
      .create();

    private final Iterable<LogHandler> logHandlers;

    private LogMessageCallback(Iterable<LogHandler> logHandlers) {
      this.logHandlers = logHandlers;
    }

    @Override
    public void onReceived(Iterator<FetchedMessage> messages) {
      while (messages.hasNext()) {
        String json = Charsets.UTF_8.decode(messages.next().getPayload()).toString();
        try {
          LogEntry entry = GSON.fromJson(json, LogEntry.class);
          if (entry != null) {
            invokeHandlers(entry);
          }
        } catch (Exception e) {
          LOG.error("Failed to decode log entry {}", json, e);
        }
      }
    }

    @Override
    public void finished() {
      // No-op
    }

    private void invokeHandlers(LogEntry entry) {
      for (LogHandler handler : logHandlers) {
        try {
          handler.onLog(entry);
        } catch (Throwable t) {
          LOG.warn("Exception while calling LogHandler {}", handler, t);
        }
      }
    }
  }
}
