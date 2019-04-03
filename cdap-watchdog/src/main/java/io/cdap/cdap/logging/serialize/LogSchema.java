/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.logging.serialize;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Handles generation of schema for logging.
 */
public final class LogSchema {

  private static final Logger LOG = LoggerFactory.getLogger(LogSchema.class);

  /**
   * Contains {@link Schema} for logging event.
   */
  public static final class LoggingEvent {
    public static final Schema SCHEMA = loadSchema("logging/schema/LoggingEvent.avsc");
  }

  private static Schema loadSchema(String resource) {
    URL url = LogSchema.class.getClassLoader().getResource(resource);
    if (url == null) {
      // This shouldn't happen
      LOG.error("Failed to find schema resource at " + resource);
      throw new IllegalStateException("Failed to find schema resource at " + resource);
    }

    try (InputStream input = url.openStream()) {
      return new Schema.Parser().parse(input);
    } catch (IOException e) {
      // This shouldn't happen
      LOG.error("Failed to load schema at " + resource);
      throw new IllegalStateException("Failed to load schema at " + resource);
    }
  }

  private LogSchema() {
  }
}
