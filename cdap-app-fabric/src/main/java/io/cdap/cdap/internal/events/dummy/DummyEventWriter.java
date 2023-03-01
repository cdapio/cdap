/*
 * Copyright © 2022 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.events.dummy;

import com.google.inject.Inject;
import io.cdap.cdap.spi.events.Event;
import io.cdap.cdap.spi.events.EventWriter;
import io.cdap.cdap.spi.events.EventWriterContext;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dummy implementation of {@link EventWriter} mainly for test proposals.
 */
public class DummyEventWriter implements EventWriter {

  private static final Logger logger = LoggerFactory.getLogger(DummyEventWriter.class);

  @Inject
  public DummyEventWriter() {
  }

  @Override
  public String getID() {
    return "DummyEventWriter-01";
  }

  @Override
  public void initialize(EventWriterContext eventWriterContext) {
    logger.info("Initializing DummyEventWriter...");
  }

  @Override
  public void write(Collection<? extends Event<?>> events) {
    events.forEach(event -> logger.info("Event: " + event));
  }

  @Override
  public void close() throws Exception {
    logger.info("Closing dummy writer");
  }
}
