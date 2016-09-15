/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal;

import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;

/**
 * Default Ids to use in test if you do not want to construct your own.
 */
public class DefaultId {
  private static final String DEFAULT_APPLICATION_ID = "myapp";

  public static final NamespaceId NAMESPACE = NamespaceId.DEFAULT;
  public static final ApplicationId APPLICATION = NAMESPACE.app(DEFAULT_APPLICATION_ID);
  public static final ArtifactId ARTIFACT = NAMESPACE.artifact("test-artifact", "0.0.0");
}
