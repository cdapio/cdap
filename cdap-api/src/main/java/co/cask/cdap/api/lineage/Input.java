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
package co.cask.cdap.api.lineage;

import co.cask.cdap.api.data.schema.Schema;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents input to the field level operation.
 */
public class Input {
  private final Schema.Field field;
  private final Source source;

  private Input(Schema.Field field, Source source) {
    this.field = field;
    this.source = source;
  }

  /**
   * Create an Input from the specified field.
   * @param field the field representing the input
   * @return the Input
   */
  public static Input ofField(Schema.Field field) {
    return ofField(field, null);
  }

  /**
   * Create an Input from the specified field belonging to the given source.
   * @param field the field representing the input
   * @param source the {@link Source} from where the field is coming from. As a part of
   *               field level operations, field will get transformed into different field.
   *               Provide source ONLY when the field is directly read from the Source, otherwise
   *               it should be set to {@code null}
   * @return
   */
  public static Input ofField(Schema.Field field, @Nullable Source source) {
    return new Input(field, source);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Input that = (Input) o;

    return Objects.equals(field, that.field) && Objects.equals(source, that.source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, source);
  }
}
