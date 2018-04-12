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
import java.util.concurrent.TimeUnit;

/**
 * Transfer records from programstatusrecordevent topic to files
 */
public class TMSSubscriber extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(TMSSubscriber.class);
  private static final String TOPIC = "programstatusrecordevent";
  private static final String NAMESPACE_SYSTEM = "system";

  private final MessageFetcher messageFetcher;
  private final RunMetaFileManager runMetaFileManager;
  private final Location baseLocation;

  private volatile boolean isStopped;

  TMSSubscriber(MessageFetcher messageFetcher, Location baseLocation) {
    super("TMS-RunrecordEvent-Subscriber-thread");
    this.messageFetcher = messageFetcher;
    isStopped = false;
    this.baseLocation = baseLocation;
    this.runMetaFileManager = new RunMetaFileManager(baseLocation);
  }

  public void requestStop() {
    isStopped = true;
    runMetaFileManager.cleanup();
    LOG.info("Shutting down tms-subscriber thread");
  }

  @Override
  public void run() {
    String afterMessageId = null;
    // todo do this with retry
    try {
      afterMessageId = MessageUtil.findMessageId(baseLocation);
    } catch (IOException e) {
      LOG.error("Exception while trying to find messageId, please fix and restart the program", e);
      return;
    }

    while (!isStopped) {
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        break;
      }

      try (CloseableIterator<Message> messageCloseableIterator =
             messageFetcher.fetch(NAMESPACE_SYSTEM, TOPIC, 10, afterMessageId)) {
        while (!isStopped && messageCloseableIterator.hasNext()) {
          Message message  = messageCloseableIterator.next();
          ProgramRunIdFields programRunIdFields = MessageUtil.constructAndGetProgramRunIdFields(message);

          runMetaFileManager.append(programRunIdFields);
          afterMessageId = message.getId();
        }
        runMetaFileManager.syncOutputStreams();
      } catch (TopicNotFoundException tpe) {
        LOG.error("Unable to find topic {} in tms, returning, cant write to the Fileset, Please fix", TOPIC, tpe);
        break;
      } catch (InterruptedException ie) {
        break;
      } catch (Exception e) {
        LOG.error("***Exception during fetching from TMS", e);
        break;
      }
    }
    LOG.info("Done reading from tms meta");
  }
}
