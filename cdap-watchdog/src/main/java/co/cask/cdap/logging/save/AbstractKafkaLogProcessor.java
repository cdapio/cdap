/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import co.cask.cdap.logging.kafka.KafkaLogEvent;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * Abstract Log Processor that stores offsets for paritions and checks if a given event is already processed.
 */
public abstract class AbstractKafkaLogProcessor implements KafkaLogProcessor {
  private Checkpoint checkpoint;

  public void init(@Nullable Checkpoint checkpoint) {
    this.checkpoint = checkpoint;
  }

  public void process(Iterator<KafkaLogEvent> events) {
    PeekingIterator<KafkaLogEvent> peekingIterator = Iterators.peekingIterator(events);
    // Find out if events have already been processed based on checkpoint
    while (peekingIterator.hasNext()) {
      KafkaLogEvent event = peekingIterator.peek();
      if (alreadyProcessed(event)) {
        peekingIterator.next();
      } else {
        break;
      }
    }

    // There are events that are not processed
    if (peekingIterator.hasNext()) {
      doProcess(peekingIterator);
    }
  }

  /**
   * doProcess method will be called if the event is not already processed.
   *
   * @param event KafkaLogEvent
   */
  protected abstract void doProcess(Iterator<KafkaLogEvent> event);

  public boolean alreadyProcessed(KafkaLogEvent event) {
    // If no checkpoint is found, then the event needs to be processed.
    // if the event offset is less than or equal to what is already checkpointed then the event is already processed
    return checkpoint != null && event.getNextOffset() <= checkpoint.getNextOffset();
  }
}
