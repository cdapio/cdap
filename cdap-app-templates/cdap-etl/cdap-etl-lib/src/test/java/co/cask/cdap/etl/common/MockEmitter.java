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

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Mock emitter for unit tests
 */
public class MockEmitter<T> implements Emitter<T> {
  private final List<T> emitted = Lists.newArrayList();
  private final List<InvalidEntry<T>> errors = Lists.newArrayList();

  @Override
  public void emit(T value) {
    emitted.add(value);
  }

  @Override
  public void emitError(InvalidEntry<T> value) {
    errors.add(value);
  }

  public List<T> getEmitted() {
    return emitted;
  }
  public List<InvalidEntry<T>> getErrors() {
    return errors;
  }

  public void clear() {
    emitted.clear();
    errors.clear();
  }
}
