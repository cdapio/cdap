/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.pipeline;

import co.cask.cdap.pipeline.Context;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Concrete implementation of {@link Context} for moving data from downstream
 * and to upstream stages.
 */
public final class StageContext implements Context {
  private final Map<String, Object> properties;
  private final Object upStream;
  private Object downStream;

  /**
   * Creates a new instance by copying the given {@link StageContext} downstream object as the new upstream object.
   * It also copy all the properties from the given {@link StageContext}.
   */
  public static StageContext next(StageContext context) {
    return new StageContext(context.downStream, context.properties);
  }

  /**
   * Constructor constructed when the result is available from upstream.
   *
   * @param upStream Object data
   */
  public StageContext(Object upStream) {
    this(upStream, Collections.<String, Object>emptyMap());
  }

  private StageContext(Object upStream, Map<String, Object> properties) {
    this.upStream = upStream;
    this.properties = new HashMap<>(properties);
  }

  /**
   * Sets result to be sent to downstream from the current stage.
   *
   * @param downStream Object to be sent to downstream
   */
  @Override
  public void setDownStream(Object downStream) {
    this.downStream = downStream;
  }

  /**
   * @return Object received from upstream
   */
  @Override
  public Object getUpStream() {
    return upStream;
  }

  /**
   * @return Object to be sent to downstream.
   */
  @Override
  public Object getDownStream() {
    return downStream;
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> T getProperty(String key) {
    return (T) properties.get(key);
  }

  @Override
  public <T> void setProperty(String key, T value) {
    properties.put(key, value);
  }

  @Override
  public Set<String> getPropertyKeys() {
    return Collections.unmodifiableSet(properties.keySet());
  }
}
