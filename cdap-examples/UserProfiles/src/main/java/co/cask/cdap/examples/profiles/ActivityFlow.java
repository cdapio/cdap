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

package co.cask.cdap.examples.profiles;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads click events from a stream, counts clicks per URL, and records user's last activity in their profiles.
 */
public class ActivityFlow implements Flow {

  private static final Logger LOG = LoggerFactory.getLogger(ActivityFlow.class);

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("ActivityFlow")
      .setDescription("Reads click events from a stream, counts clicks per URL, and records user activity.")
      .withFlowlets()
      .add("reader", new EventReader())
      .add("counter", new Counter())
      .add("updater", new Updater())
      .connect()
      .fromStream("events").to("reader")
      .from("reader").to("counter")
      .from("reader").to("updater")
      .build();
  }

  private class EventReader extends AbstractFlowlet {

    private OutputEmitter<Event> out;

    @ProcessInput
    public void process(StreamEvent streamEvent) {
      try {
        out.emit(Event.fromJson(streamEvent.getBody()));
      } catch (Exception e) {
        LOG.debug("Problem decoding event: {}", Bytes.toString(streamEvent.getBody()), e);
      }
    }
  }

  private class Counter extends AbstractFlowlet {

    @UseDataSet("counters")
    private KeyValueTable counters;
UPS
    @ProcessInput
    public void process(Event event) {
      counters.increment(Bytes.toBytes(event.getUrl()), 1L);
    }
  }

  private class Updater extends AbstractFlowlet {

    @UseDataSet("profiles")
    private Table profiles;

    @ProcessInput
    public void process(Event event) {
      String id = event.getUserId();
      if (profiles.get(new Get(id, "id")).isEmpty()) {
        LOG.debug("Received event for unknown user id {}.", id);
        return;
      }
      profiles.put(new Put(id, "active", event.getTime()));
    }
  }
}
