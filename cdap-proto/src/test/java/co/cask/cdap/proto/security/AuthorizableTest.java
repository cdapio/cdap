/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.proto.security;

import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.id.StreamId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Authorizable}
 */
public class AuthorizableTest {

  @Test
  public void testArtifact() {
    Authorizable authorizable;
    ArtifactId artifactId = new ArtifactId("ns", "art", "1.0-SNAPSHOT");
    authorizable = Authorizable.fromEntityId(artifactId);
    // drop the version while asserting
    Assert.assertEquals(artifactId.toString().replace(".1.0-SNAPSHOT", ""), authorizable.toString());
  }

  @Test
  public void testNamespace() {
    Authorizable authorizable;
    NamespaceId namespaceId = new NamespaceId("ns");
    authorizable = Authorizable.fromEntityId(namespaceId);
    Assert.assertEquals(namespaceId.toString(), authorizable.toString());

    String wildcardNs = namespaceId.toString() + "?";
    authorizable = Authorizable.fromString(wildcardNs);
    Assert.assertEquals(wildcardNs, authorizable.toString());

    wildcardNs = namespaceId.toString() + "*" + "more";
    authorizable = Authorizable.fromString(wildcardNs);
    Assert.assertEquals(wildcardNs, authorizable.toString());
  }

  @Test
  public void testProgram() {
    ProgramId programId = new ProgramId("ns", "app", ProgramType.MAPREDUCE, "prog");
    Authorizable authorizable = Authorizable.fromEntityId(programId);
    // drop the version while asserting
    Assert.assertEquals(programId.toString().replace(ApplicationId.DEFAULT_VERSION + ".", ""), authorizable.toString());

    // test fromString with version should throw exception
    String wildCardProgramId = programId.toString() + "*";
    try {
      Authorizable.fromString(wildCardProgramId);
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // expected
    }

    ApplicationId appId = new ApplicationId("ns", "app", "1.0-SNAPSHOT");
    programId = appId.program(ProgramType.MAPREDUCE, "prog");
    authorizable = Authorizable.fromEntityId(programId);
    // drop the version while asserting
    Assert.assertEquals(programId.toString().replace(".1.0-SNAPSHOT", ""), authorizable.toString());
  }

  @Test
  public void testApplication() {
    ApplicationId appId = new ApplicationId("ns", "app", "1.0-SNAPSHOT");
    Authorizable authorizable = Authorizable.fromEntityId(appId);
    // drop the version while asserting
    Assert.assertEquals(appId.toString().replace(".1.0-SNAPSHOT", ""), authorizable.toString());

    try {
      Authorizable.fromString(appId.toString());
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  @Test
  public void testPrincipal() {
    KerberosPrincipalId kerberosPrincipalId = new KerberosPrincipalId("eve/host1.com@domain.net");
    Authorizable authorizable = Authorizable.fromEntityId(kerberosPrincipalId);
    Assert.assertEquals(kerberosPrincipalId.toString(), authorizable.toString());
  }

  @Test
  public void testStream() {
    StreamId streamId = new StreamId("ns", "stream");
    Authorizable authorizable = Authorizable.fromEntityId(streamId);
    Assert.assertEquals(streamId.toString(), authorizable.toString());
  }

  @Test
  public void testDataset() {
    DatasetId datasetId = new DatasetId("ns", "dataset");
    Authorizable authorizable = Authorizable.fromEntityId(datasetId);
    Assert.assertEquals(datasetId.toString(), authorizable.toString());
  }

  @Test
  public void testSecureKey() {
    SecureKeyId secureKeyId = new SecureKeyId("ns", "secure");
    Authorizable authorizable = Authorizable.fromEntityId(secureKeyId);
    Assert.assertEquals(secureKeyId.toString(), authorizable.toString());
  }

  @Test
  public void testDatasetType() {
    DatasetTypeId datasetTypeId = new DatasetTypeId("ns", "datasetType");
    Authorizable authorizable = Authorizable.fromEntityId(datasetTypeId);
    Assert.assertEquals(datasetTypeId.toString(), authorizable.toString());
  }

  @Test
  public void testDatasetModule() {
    DatasetModuleId datasetModuleId = new DatasetModuleId("ns", "datasetModule");
    Authorizable authorizable = Authorizable.fromEntityId(datasetModuleId);
    Assert.assertEquals(datasetModuleId.toString(), authorizable.toString());
  }
}
