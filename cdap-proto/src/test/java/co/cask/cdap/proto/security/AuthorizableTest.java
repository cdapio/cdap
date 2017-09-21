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
  private static final String ILLEGAL_MSG = "Entity value is missing some parts or containing more parts";
  private static final String UNSUPPORTED_MSG = "Privilege can only be granted at the artifact/application level.";

  @Test
  public void testArtifact() {
    Authorizable authorizable;
    ArtifactId artifactId = new ArtifactId("ns", "art", "1.0-SNAPSHOT");
    authorizable = Authorizable.fromEntityId(artifactId);
    // drop the version while asserting
    String artifactIdNoVer = artifactId.toString().replace(".1.0-SNAPSHOT", "");
    Assert.assertEquals(artifactIdNoVer, authorizable.toString());

    String widcardId = artifactIdNoVer.replace("est", "*es?t");
    Assert.assertEquals(widcardId, Authorizable.fromString(widcardId).toString());

    verifyInvalidString("artifact:art");
    verifyInvalidString("artifact:ns.art.1.0-SNAPSHOT");
  }

  @Test
  public void testNamespace() {
    Authorizable authorizable;
    NamespaceId namespaceId = new NamespaceId("test_ns");
    authorizable = Authorizable.fromEntityId(namespaceId);
    Assert.assertEquals(namespaceId.toString(), authorizable.toString());

    String wildcardNs = namespaceId.toString() + "?";
    authorizable = Authorizable.fromString(wildcardNs);
    Assert.assertEquals(wildcardNs, authorizable.toString());

    wildcardNs = namespaceId.toString() + "*" + "more";
    authorizable = Authorizable.fromString(wildcardNs);
    Assert.assertEquals(wildcardNs, authorizable.toString());

    String widcardId = namespaceId.toString().replace("est", "*es?t");
    Assert.assertEquals(widcardId, Authorizable.fromString(widcardId).toString());

    verifyInvalidString("namespace:ns.ns1");
  }

  @Test
  public void testProgram() {
    ProgramId programId = new ProgramId("ns", "app", ProgramType.MAPREDUCE, "prog");
    Authorizable authorizable = Authorizable.fromEntityId(programId);
    // drop the version while asserting
    Assert.assertEquals(programId.toString().replace(ApplicationId.DEFAULT_VERSION + ".", ""), authorizable.toString());

    // test fromString with version should throw exception
    String wildCardProgramId = programId.toString() + "*";
    verifyInvalidString(wildCardProgramId);

    ApplicationId appId = new ApplicationId("ns", "app", "1.0-SNAPSHOT");
    programId = appId.program(ProgramType.MAPREDUCE, "prog");
    authorizable = Authorizable.fromEntityId(programId);
    // drop the version while asserting
    String programIdNoVer = programId.toString().replace(".1.0-SNAPSHOT", "");
    Assert.assertEquals(programIdNoVer, authorizable.toString());

    String widcardId = programIdNoVer.replace("est", "*es?t");
    Assert.assertEquals(widcardId, Authorizable.fromString(widcardId).toString());

    String allProgs = "program:ns.app.*";
    Assert.assertEquals(allProgs, Authorizable.fromString(allProgs).toString());

    verifyInvalidString("program:ns.app");
    verifyInvalidString("program:test:*.*");
  }

  @Test
  public void testApplication() {
    ApplicationId appId = new ApplicationId("ns", "app", "1.0-SNAPSHOT");
    Authorizable authorizable = Authorizable.fromEntityId(appId);
    // drop the version while asserting
    String appIdNoVer = appId.toString().replace(".1.0-SNAPSHOT", "");
    Assert.assertEquals(appIdNoVer, authorizable.toString());

    verifyInvalidString(appId.toString());

    String widcardId = appIdNoVer.replace("est", "*es?t");
    Assert.assertEquals(widcardId, Authorizable.fromString(widcardId).toString());

    verifyInvalidString("application:app");
  }

  @Test
  public void testPrincipal() {
    KerberosPrincipalId kerberosPrincipalId = new KerberosPrincipalId("eve/host*.com@domai?.net");
    Authorizable authorizable = Authorizable.fromEntityId(kerberosPrincipalId);
    Assert.assertEquals(kerberosPrincipalId.toString(), authorizable.toString());

    Assert.assertEquals(kerberosPrincipalId.toString() + "*.com",
                        Authorizable.fromString(authorizable.toString() + "*.com").toString());
  }

  @Test
  public void testStream() {
    StreamId streamId = new StreamId("ns", "test_stream");
    Authorizable authorizable = Authorizable.fromEntityId(streamId);
    Assert.assertEquals(streamId.toString(), authorizable.toString());

    String widcardId = streamId.toString().replace("est", "*es?t");
    Assert.assertEquals(widcardId, Authorizable.fromString(widcardId).toString());

    verifyInvalidString("stream:ns");
    verifyInvalidString("stream:ns.stream1.invalid");
    verifyInvalidString("stream:ns:stream1");
  }

  @Test
  public void testDataset() {
    DatasetId datasetId = new DatasetId("ns", "co.cask.test_dataset");
    Authorizable authorizable = Authorizable.fromEntityId(datasetId);
    Assert.assertEquals(datasetId.toString(), authorizable.toString());

    String widcardId = datasetId.toString().replace("est", "*es?t");
    Assert.assertEquals(widcardId, Authorizable.fromString(widcardId).toString());

    verifyInvalidString("dataset:test");
    verifyInvalidString("dataset:ns:test");
  }

  @Test
  public void testSecureKey() {
    SecureKeyId secureKeyId = new SecureKeyId("ns", "test_secure");
    Authorizable authorizable = Authorizable.fromEntityId(secureKeyId);
    Assert.assertEquals(secureKeyId.toString(), authorizable.toString());

    String widcardId = secureKeyId.toString().replace("est", "*es?t");
    Assert.assertEquals(widcardId, Authorizable.fromString(widcardId).toString());
  }

  @Test
  public void testDatasetType() {
    DatasetTypeId datasetTypeId = new DatasetTypeId("ns", "co.cask.test_datasetType");
    Authorizable authorizable = Authorizable.fromEntityId(datasetTypeId);
    Assert.assertEquals(datasetTypeId.toString(), authorizable.toString());

    String widcardId = datasetTypeId.toString().replace("est", "*es?t");
    Assert.assertEquals(widcardId, Authorizable.fromString(widcardId).toString());
  }

  @Test
  public void testDatasetModule() {
    DatasetModuleId datasetModuleId = new DatasetModuleId("ns", "co.cask.test_datasetModule");
    Authorizable authorizable = Authorizable.fromEntityId(datasetModuleId);
    Assert.assertEquals(datasetModuleId.toString(), authorizable.toString());

    String widcardId = datasetModuleId.toString().replace("est", "*es?t");
    Assert.assertEquals(widcardId, Authorizable.fromString(widcardId).toString());
  }

  private void verifyInvalidString(String invalidString) {
    try {
      Authorizable.fromString(invalidString);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(ILLEGAL_MSG));
    } catch (UnsupportedOperationException e) {
      Assert.assertTrue(e.getMessage().contains(UNSUPPORTED_MSG));
    }
  }
}
