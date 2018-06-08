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

package co.cask.cdap.proto.metadata.lineage;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;
/**
 * Represents input to the field operation.
 */
public class FieldOperationInput {
  private final List<EndPoint> endPoints;
  private final List<InputField> fields;

  public FieldOperationInput(@Nullable List<EndPoint> endPoints, @Nullable List<InputField> fields) {
    this.endPoints = endPoints == null ? null : Collections.unmodifiableList(new ArrayList<>(endPoints));
    this.fields = fields == null ? null : Collections.unmodifiableList(new ArrayList<>(fields));
  }

  @Nullable
  public List<EndPoint> getEndPoints() {
    return endPoints;
  }

  @Nullable
  public List<InputField> getFields() {
    return fields;
  }
}
