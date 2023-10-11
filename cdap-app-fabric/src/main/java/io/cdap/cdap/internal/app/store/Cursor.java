/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.table.field.Range.Bound;
import java.util.Collection;
import java.util.Collections;

/**
 * Represents a position for scanning.
 */
public class Cursor {

  public static final Cursor EMPTY = new Cursor(Collections.emptyList(), Range.Bound.INCLUSIVE);

  private final Collection<Field<?>> fields;
  private final Range.Bound bound;

  public Cursor(Collection<Field<?>> fields, Range.Bound bound) {
    this.fields = fields;
    this.bound = bound;
  }

  public Collection<Field<?>> getFields() {
    return fields;
  }

  public Bound getBound() {
    return bound;
  }
}
