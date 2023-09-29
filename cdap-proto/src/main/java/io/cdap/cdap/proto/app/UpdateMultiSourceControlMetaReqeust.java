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

package io.cdap.cdap.proto.app;

import java.util.Collection;
import java.util.Objects;

/**
 * Request class to update source control meta of multiple applications.
 */
public class UpdateMultiSourceControlMetaReqeust {
  private final Collection<UpdateSourceControlMetaRequest> apps;

  public UpdateMultiSourceControlMetaReqeust(Collection<UpdateSourceControlMetaRequest> apps) {
    this.apps = apps;
  }

  public Collection<UpdateSourceControlMetaRequest> getApps() {
    return apps;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UpdateMultiSourceControlMetaReqeust)) {
      return false;
    }
    UpdateMultiSourceControlMetaReqeust that = (UpdateMultiSourceControlMetaReqeust) o;
    return Objects.equals(apps, that.apps);
  }

  @Override
  public int hashCode() {
    return Objects.hash(apps);
  }
}
