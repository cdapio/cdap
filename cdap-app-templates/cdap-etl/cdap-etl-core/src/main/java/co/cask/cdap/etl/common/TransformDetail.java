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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transformation;

import java.util.Collection;

/**
 * Encapsulates {@link Transformation} list of next stages, current stage name, and {@link DefaultEmitter}.
 */
public class TransformDetail implements Emitter<Object> {
  private final Transformation transformation;
  private final Collection<String> nextStages;
  private final DefaultEmitter defaultEmitter;

  public TransformDetail(Transformation transformation, StageMetrics metrics, Collection<String> nextStages) {
    this.transformation = new TrackedTransform<>(transformation, metrics);
    this.nextStages = nextStages;
    this.defaultEmitter = new DefaultEmitter<>(metrics);
  }

  @Override
  public void emit(Object value) {
    this.defaultEmitter.emit(value);
  }

  @Override
  public void emitError(InvalidEntry<Object> invalidEntry) {
    this.defaultEmitter.emitError(invalidEntry);
  }

  public Collection<Object> getEntries() {
    return defaultEmitter.getEntries();
  }

  public Collection<InvalidEntry<Object>> getErrors() {
    return defaultEmitter.getErrors();
  }

  public void resetEmitter() {
    defaultEmitter.reset();
  }

  public void destroy() {
    if (transformation instanceof Destroyable) {
      Destroyables.destroyQuietly((Destroyable) transformation);
    }
  }

  public Transformation getTransformation() {
    return transformation;
  }

  /**
   * @return the list of next stages from this stage; for sinks this list is empty
   */
  public Collection<String> getNextStages() {
    return nextStages;
  }

}
