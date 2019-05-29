/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.store.artifact.schema;

import io.cdap.cdap.graphql.schema.Types;

public class ArtifactTypes implements Types {

  public static final String ARTIFACT_DETAIL = "ArtifactDetail";

  private ArtifactTypes() {
    throw new UnsupportedOperationException("Helper class should not be instantiated");
  }

}
