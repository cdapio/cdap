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

package co.cask.cdap.pipeline;

import com.google.common.reflect.TypeToken;

/**
 * Abstract implementation of {@link Stage} allowing ability to determine type
 * to invoke the actual processing of event.
 *
 * @param <T> Type of object processed by this stage.
 */
public abstract class AbstractStage<T> implements Stage {
  private Context ctx;
  private TypeToken<T> typeToken;

  /**
   * Constructor that allows determining the type {@link Stage} is looking to process.
   *
   * @param typeToken instance to determine type of data {@link Stage} is processing.
   */
  protected AbstractStage(TypeToken<T> typeToken) {
    this.typeToken = typeToken;
  }

  /**
   * Processes an object passed to it from context.
   *
   * @param ctx of processing.
   */
  @SuppressWarnings("unchecked")
  public final void process(Context ctx) throws Exception {
    this.ctx = ctx;
    Object upStream = ctx.getUpStream();
    if (typeToken.getRawType().isAssignableFrom(upStream.getClass())) {
      process((T) typeToken.getRawType().cast(upStream));
    }
  }

  /**
   * Emits the object to send to next {@link Stage} in processing.
   *
   * @param o to be emitted to downstream
   */
  protected final void emit(Object o) {
    ctx.setDownStream(o);
  }

  /**
   * Abstract process that does a safe cast to the type.
   *
   * @param o Object to be processed which is of type T
   */
  public abstract void process(T o) throws Exception;
}
