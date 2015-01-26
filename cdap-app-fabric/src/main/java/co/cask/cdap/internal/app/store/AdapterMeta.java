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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.internal.app.runtime.adapter.AdapterStatus;
import co.cask.cdap.proto.AdapterSpecification;

/**
 * Holds adapter metadata
 */
public class AdapterMeta {
  private final AdapterSpecification spec;
  private final AdapterStatus status;

  public AdapterMeta(AdapterSpecification spec, AdapterStatus status) {
    this.spec = spec;
    this.status = status;
  }

  public static AdapterMeta updateStatus(AdapterMeta original, AdapterStatus newStatus) {
    return new AdapterMeta(original.spec, newStatus);
  }

  public AdapterSpecification getSpec() {
    return spec;
  }

  public AdapterStatus getStatus() {
    return status;
  }

}
