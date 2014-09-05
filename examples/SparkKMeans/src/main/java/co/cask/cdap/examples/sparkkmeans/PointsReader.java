/*
 * Copyright 2014 Cask Data, Inc.
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
package co.cask.cdap.examples.sparkkmeans;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * This Flowlet reads events from a Stream and saves them to a datastore.
 */
public class PointsReader extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(PointsReader.class);

  @UseDataSet("points")
  private ObjectStore<String> pointsStore;

  @ProcessInput
  public void process(StreamEvent event) {
    String body = new String(event.getBody().array());
    LOG.trace("Points info: {}", body);
    pointsStore.write(getIdAsByte(UUID.randomUUID()), body);
  }

  private static byte[] getIdAsByte(UUID uuid) {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }
}
