/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.framework;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.WarnStatus;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.logging.meta.FileMetaDataWriter;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Log Appender implementation for CDAP Log framework
 * TODO : Refactor package CDAP-8196
 */
public class CDAPLogAppender extends AppenderBase<ILoggingEvent> implements Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(CDAPLogAppender.class);
  private static final String TAG_NAMESPACE_ID = ".namespaceId";
  private static final String TAG_APPLICATION_ID = ".applicationId";
  private static final String TAG_FLOW_ID = ".flowId";
  private static final String TAG_SERVICE_ID = ".serviceId";
  private static final String TAG_MAP_REDUCE_JOB_ID = ".mapReduceId";
  private static final String TAG_SPARK_JOB_ID = ".sparkId";
  private static final String TAG_USER_SERVICE_ID = ".userserviceid";
  private static final String TAG_WORKER_ID = ".workerid";
  private static final String TAG_WORKFLOW_ID = ".workflowId";

  private LogFileManager logFileManager;

  private int syncIntervalBytes = 10 * 1024 * 1024;             // Default to 10M, can be set in logback
  private long maxFileLifetimeMs = TimeUnit.HOURS.toMillis(6);  // Default to 6 hourse, can be set in logback

  /**
   * TODO: start a separate cleanup thread to remove files that has passed the TTL
   */
  public CDAPLogAppender() {
    setName(getClass().getName());
  }

  public void setSyncIntervalBytes(int syncIntervalBytes) {
    this.syncIntervalBytes = syncIntervalBytes;
  }

  public void setMaxFileLifetimeMs(long maxFileLifetimeMs) {
    this.maxFileLifetimeMs = maxFileLifetimeMs;
  }

  @Override
  public void start() {
    super.start();
    Preconditions.checkState(syncIntervalBytes > 0, "Property syncIntervalBytes must be > 0.");
    Preconditions.checkState(maxFileLifetimeMs > 0, "Property maxFileLifetimeMs must be > 0");
    // This shouldn't happen
    Preconditions.checkState(context instanceof AppenderContext,
                             "The context object is not an instance of %s", AppenderContext.class);

    AppenderContext context = (AppenderContext) this.context;
    logFileManager = new LogFileManager(maxFileLifetimeMs, syncIntervalBytes, LogSchema.LoggingEvent.SCHEMA,
                                        new FileMetaDataWriter(context.getDatasetManager(), context),
                                        context.getLocationFactory());
  }

  @Override
  public void doAppend(ILoggingEvent eventObject) throws LogbackException {
    long timestamp = eventObject.getTimeStamp();
    try {
      // logic from AppenderBase
      if (!this.started) {
        addStatus(new WarnStatus(
          "Attempted to append to non started appender [" + name + "].",
          this));
        return;
      }

      // logic from AppenderBase
      if (getFilterChainDecision(eventObject) == FilterReply.DENY) {
        return;
      }

      LogPathIdentifier logPathIdentifier = getLoggingPath(eventObject.getMDCPropertyMap());
      LogFileOutputStream outputStream = logFileManager.getLogFileOutputStream(logPathIdentifier, timestamp);
      outputStream.append(eventObject);
    } catch (IllegalArgumentException iae) {
      // this shouldn't happen
      LOG.error("Unrecognized context ", iae);
    } catch (IOException ioe) {
      throw new LogbackException("Exception during append", ioe);
    }
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    // no-op - this wont be called as we are overriding doAppend
  }


  @Override
  public void flush() throws IOException {
    logFileManager.flush();
  }

  @Override
  public void stop() {
    try {
      logFileManager.close();
    } finally {
      super.stop();
    }
  }

  @VisibleForTesting
  LogPathIdentifier getLoggingPath(Map<String, String> propertyMap) throws IllegalArgumentException {
    // from the property map, get namespace values
    // if the namespace is system : get component-id and return that as path
    // if the namespace is non-system : get "app" and "program-name" and return that as path

    String namespaceId = propertyMap.get(TAG_NAMESPACE_ID);

    if (NamespaceId.SYSTEM.getNamespace().equals(namespaceId)) {
      Preconditions.checkArgument(propertyMap.containsKey(TAG_SERVICE_ID),
                                  String.format("%s is expected but not found in the context %s",
                                                TAG_SERVICE_ID, propertyMap));
      // adding services to be consistent with the old format
      return new LogPathIdentifier(namespaceId, Constants.Logging.COMPONENT_NAME, propertyMap.get(TAG_SERVICE_ID));
    } else {
      Preconditions.checkArgument(propertyMap.containsKey(TAG_APPLICATION_ID),
                                  String.format("%s is expected but not found in the context %s",
                                                TAG_APPLICATION_ID, propertyMap));
      String application = propertyMap.get(TAG_APPLICATION_ID);

      List<String> programIdKeys = Arrays.asList(TAG_FLOW_ID, TAG_MAP_REDUCE_JOB_ID, TAG_SPARK_JOB_ID,
                                                 TAG_USER_SERVICE_ID, TAG_WORKER_ID, TAG_WORKFLOW_ID);
      String program = null;
      for (String programId : programIdKeys) {
        if (propertyMap.containsKey(programId)) {
          program = propertyMap.get(programId);
          break;
        }
      }
      Preconditions.checkArgument(program != null, String.format("Unrecognized program in the context %s",
                                                                 propertyMap));
      return new LogPathIdentifier(namespaceId, application, program);
    }
  }
}
