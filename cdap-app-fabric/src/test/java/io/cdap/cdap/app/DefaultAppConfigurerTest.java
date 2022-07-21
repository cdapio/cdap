/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.app;

import io.cdap.cdap.DummyAppWithTrackingTable;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

public class DefaultAppConfigurerTest {
  @Test
  public void createSpecification() {
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("test", "1.0.0");
    DefaultAppConfigurer configurer = new DefaultAppConfigurer(Id.Namespace.DEFAULT,
                                                               Id.Artifact.fromEntityId(artifactId),
                                                               new DummyAppWithTrackingTable());
    ApplicationSpecification appSpec = configurer.createSpecification(null);
    Assert.assertEquals(ProjectInfo.getVersion().toString(), appSpec.getAppCDAPVersion());
  }
}
