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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import com.google.common.base.Function;

import java.util.Collection;
import java.util.Map;

/**
 * Wrapper around another emitter that will transform objects before emitting them.
 * This is meant to be used in situations where the emitter is passed to something that emits objects
 * of type EMIT, but we need to eventually fetch objects of type FETCH. For example, a BatchSink
 * emits KeyValue objects, but in some Spark classes, we want to get back Tuple2 objects.
 *
 * @param <EMIT> the type of objects emitted
 * @param <FETCH> the type of objects fetched
 */
public class TransformingEmitter<EMIT, FETCH> implements Emitter<EMIT> {
  private final DefaultEmitter<FETCH> emitter;
  private final Function<EMIT, FETCH> function;

  public TransformingEmitter(Function<EMIT, FETCH> function) {
    this(new DefaultEmitter<FETCH>(), function);
  }

  public TransformingEmitter(DefaultEmitter<FETCH> emitter, Function<EMIT, FETCH> function) {
    this.emitter = emitter;
    this.function = function;
  }

  @Override
  public void emit(EMIT value) {
    emitter.emit(function.apply(value));
  }

  @Override
  public void emitError(InvalidEntry<EMIT> value) {
    emitter.emitError(new InvalidEntry<>(value.getErrorCode(), value.getErrorMsg(),
                                         function.apply(value.getInvalidRecord())));
  }

  @Override
  public void emitAlert(Map<String, String> payload) {
    emitter.emitAlert(payload);
  }

  public Collection<FETCH> getEntries() {
    return emitter.getEntries();
  }

  public Collection<InvalidEntry<FETCH>> getErrors() {
    return emitter.getErrors();
  }

  public void reset() {
    emitter.reset();
  }
}
