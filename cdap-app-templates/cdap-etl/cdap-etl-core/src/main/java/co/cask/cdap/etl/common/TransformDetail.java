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
import co.cask.cdap.etl.api.Transformation;

import java.util.Collection;

/**
 * Encapsulates {@link Transformation} list of next stages, current stage name, and {@link DefaultEmitter}.
 * @param <T> the type of object to emit
 */
public class TransformDetail<T> implements Emitter<T> {
  private final Transformation transformation;
  private final Collection<String> nextStages;
  private String prevStage;
  private final DefaultEmitter<T> defaultEmitter;

  public TransformDetail(Transformation transformation, Collection<String> nextStages) {
    this.transformation = transformation;
    this.nextStages = nextStages;
    this.defaultEmitter = new DefaultEmitter<>();
    this.prevStage = "";
  }

  public String getPrevStage() {
    return prevStage;
  }

  public void setPrevStage(String prevStage) {
    this.prevStage = prevStage;
  }

  @Override
  public void emit(T value) {
    this.defaultEmitter.emit(value);
  }

  @Override
  public void emitError(InvalidEntry<T> invalidEntry) {
    this.defaultEmitter.emitError(invalidEntry);
  }

  public Collection<T> getEntries() {
    return defaultEmitter.getEntries();
  }

  public Collection<InvalidEntry<T>> getErrors() {
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
