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
package co.cask.cdap.report.main;


import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Transfer records from programstatusrecordevent topic to files
 */
public class TMSSubscriber extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(TMSSubscriber.class);
  private static final String TOPIC = "programstatusrecordevent";
  private static final String NAMESPACE_SYSTEM = "system";
  private static final String FETCH_SIZE = "tms.fetch.size";
  private static final int DEFAULT_FETCH_SIZE = 100;

  private final MessageFetcher messageFetcher;
  private final RunMetaFileManager runMetaFileManager;
  private final Location baseLocation;
  private final int fetchSize;

  private volatile boolean isStopped;

  TMSSubscriber(MessageFetcher messageFetcher, Location baseLocation, Map<String, String> runtimeArguments) {
    super("TMS-RunrecordEvent-Subscriber-thread");
    this.messageFetcher = messageFetcher;
    isStopped = false;
    this.baseLocation = baseLocation;
    this.runMetaFileManager = new RunMetaFileManager(baseLocation, runtimeArguments);
    this.fetchSize = runtimeArguments.containsKey(FETCH_SIZE) ?
      Integer.parseInt(runtimeArguments.get(FETCH_SIZE)) : DEFAULT_FETCH_SIZE;
  }

  public void requestStop() {
    isStopped = true;
    runMetaFileManager.cleanup();
    LOG.info("Shutting down tms-subscriber thread");
  }

  @Override
  public void run() {
    String afterMessageId = null;
    SampledLogging sampledLogging  = new SampledLogging(LOG, 100);
    try {
      afterMessageId = MessageUtil.findMessageId(baseLocation);
    } catch (InterruptedException e) {
      return;
    }
    while (!isStopped) {
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        break;
      }
      try (CloseableIterator<Message> messageCloseableIterator =
             messageFetcher.fetch(NAMESPACE_SYSTEM, TOPIC, fetchSize, afterMessageId)) {
        while (!isStopped && messageCloseableIterator.hasNext()) {
          Message message  = messageCloseableIterator.next();
          ProgramRunInfo programRunInfo = MessageUtil.constructAndGetProgramRunInfo(message);
          runMetaFileManager.append(programRunInfo);
          afterMessageId = message.getId();
        }
        runMetaFileManager.syncOutputStreams();
      } catch (TopicNotFoundException tpe) {
        LOG.error("Unable to find topic {} in tms, returning, cant write to the Fileset, Please fix", TOPIC, tpe);
        break;
      } catch (InterruptedException ie) {
        break;
      } catch (IOException e) {
        sampledLogging.logWarning("Exception while fetching from TMS, will be retried", e);
      }
    }
    LOG.info("Done reading from tms meta");
  }
}
