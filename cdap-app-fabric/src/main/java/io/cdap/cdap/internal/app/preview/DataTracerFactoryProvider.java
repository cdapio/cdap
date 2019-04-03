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
package co.cask.cdap.internal.app.preview;

import co.cask.cdap.app.preview.DataTracerFactory;
import co.cask.cdap.proto.id.ApplicationId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class which provides {@link DataTracerFactory} based on the {@link ApplicationId}
 */
public final class DataTracerFactoryProvider {
  private static final DataTracerFactory DEFAULT_FACTORY = new NoopDataTracerFactory();
  private static final Map<ApplicationId, DataTracerFactory> FACTORY_MAP = new ConcurrentHashMap<>();

  private DataTracerFactoryProvider() {
  }

  /**
   * Set the {@link DataTracerFactory} for a {@link ApplicationId} in a map.
   */
  public static void setDataTracerFactory(ApplicationId applicationId, DataTracerFactory dataTracerFactory) {
    FACTORY_MAP.put(applicationId, dataTracerFactory);
  }

  /**
   * Get the {@link DataTracerFactory} for a {@link ApplicationId}
   */
  public static DataTracerFactory get(ApplicationId applicationId) {
    DataTracerFactory factory = FACTORY_MAP.get(applicationId);
    if (factory == null) {
      return DEFAULT_FACTORY;
    }
    return factory;
  }

  /**
   * Remove the {@link DataTracerFactory} for a {@link ApplicationId} from a map.
   */
  public static void removeDataTracerFactory(ApplicationId applicationId) {
    FACTORY_MAP.remove(applicationId);
  }
}
