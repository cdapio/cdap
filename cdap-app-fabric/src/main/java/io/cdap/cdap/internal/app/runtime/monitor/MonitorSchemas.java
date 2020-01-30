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

package io.cdap.cdap.internal.app.runtime.monitor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * Loads Avro schemas for Monitor request and response.
 */
public final class MonitorSchemas {

  private MonitorSchemas() {
    // protect the constructor
  }

  private static final Logger LOG = LoggerFactory.getLogger(MonitorSchemas.class);

  /**
   * Contains Schema for V1 protocol.
   */
  public static final class V1 {

    /**
     * Contains schema for monitor consume request.
     */
    public static final class MonitorConsumeRequest {
      public static final Schema SCHEMA = loadSchema(MonitorSchemas.V1.MonitorConsumeRequest.class);
    }

    /**
     * Contains schema for monitor response.
     */
    public static final class MonitorResponse {
      public static final Schema SCHEMA = loadSchema(MonitorSchemas.V1.MonitorResponse.class);
    }
  }

  /**
   * Contains Schema for V2 protocol.
   */
  public static final class V2 {

    /**
     * Contains schema for monitor request.
     */
    public static final class MonitorRequest {
      public static final Schema SCHEMA = loadSchema(MonitorSchemas.V2.MonitorRequest.class);
    }
  }

  /**
   * Creates a {@link Schema} instance based on the given class name.
   */
  @Nullable
  private static Schema loadSchema(Class<?> cls) {
    // The schema file is part of the classloader resource.
    String resourceName = String.format("schema/%s/%s.avsc",
                                        cls.getDeclaringClass().getSimpleName().toLowerCase(),
                                        cls.getSimpleName());
    URL resource = cls.getClassLoader().getResource(resourceName);
    if (resource == null) {
      // Shouldn't happen
      LOG.warn("Failed to load schema from resource {}", resourceName);
      throw new IllegalStateException("Failed to load schema from resource " + resourceName);
    }

    try (InputStream is = resource.openStream()) {
      Schema schema = new Schema.Parser().parse(is);
      // Avro converts java String to UTF8 while deserializing strings. Set String type to Java String
      GenericData.setStringType(schema, GenericData.StringType.String);
      return schema;
    } catch (IOException e) {
      // Shouldn't happen
      LOG.warn("Failed to read schema from resource {}", resourceName, e);
      throw new IllegalStateException("Failed to read schema from resource " + resourceName, e);
    }
  }
}
