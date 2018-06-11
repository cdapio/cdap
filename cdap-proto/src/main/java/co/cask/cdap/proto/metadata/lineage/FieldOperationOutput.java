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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.WriteOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Represents an output of a field operation: in case of a {@link WriteOperation},
 * a a dataset ({@link EndPoint}); otherwise a list of field names.
 */
@Beta
public class FieldOperationOutput {
  private final EndPoint endPoint;
  private final List<String> fields;

  public FieldOperationOutput(@Nullable EndPoint endPoint, @Nullable List<String> fields) {
    this.endPoint = endPoint;
    this.fields = fields == null ? null : Collections.unmodifiableList(new ArrayList<>(fields));
  }

  /**
   * Create an instance of {@link FieldOperationOutput} from a given EndPoint.
   *
   * @param endPoint an EndPoint representing output of the operation
   * @return instance of {@link FieldOperationOutput}
   */
  public static FieldOperationOutput of(EndPoint endPoint) {
    return new FieldOperationOutput(endPoint, null);
  }

  /**
   * Create an instance of {@link FieldOperationOutput} from a given list of fields.
   *
   * @param fields the list of fields which represents output of the operation
   * @return instance of {@link FieldOperationOutput}
   */
  public static FieldOperationOutput of(List<String> fields) {
    return new FieldOperationOutput(null, fields);
  }

  @Nullable
  public EndPoint getEndPoint() {
    return endPoint;
  }

  @Nullable
  public List<String> getFields() {
    return fields;
  }
}
