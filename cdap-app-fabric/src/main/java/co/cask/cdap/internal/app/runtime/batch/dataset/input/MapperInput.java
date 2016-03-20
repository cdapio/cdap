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

package co.cask.cdap.internal.app.runtime.batch.dataset.input;

import co.cask.cdap.api.data.batch.InputFormatProvider;
import org.apache.hadoop.mapreduce.Mapper;

import javax.annotation.Nullable;

/**
 * Encapsulates {@link InputFormatProvider} and a {@link Mapper} to use for that input.
 */
public class MapperInput {
  private final InputFormatProvider inputFormatProvider;
  private final Class<? extends Mapper> mapper;

  /**
   * Creates an instance of MapperInput with the given InputFormatProvider.
   */
  public MapperInput(InputFormatProvider inputFormatProvider) {
    this(inputFormatProvider, null);
  }

  /**
   * Creates an instance of MapperInput with the given InputFormatProvider and specified Mapper class.
   */
  public MapperInput(InputFormatProvider inputFormatProvider, @Nullable Class<? extends Mapper> mapper) {
    this.inputFormatProvider = inputFormatProvider;
    this.mapper = mapper;
  }

  public InputFormatProvider getInputFormatProvider() {
    return inputFormatProvider;
  }

  @Nullable
  public Class<? extends Mapper> getMapper() {
    return mapper;
  }
}
