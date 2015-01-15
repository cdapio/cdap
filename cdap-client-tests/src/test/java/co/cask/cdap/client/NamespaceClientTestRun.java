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

package co.cask.cdap.client;

import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.exception.AlreadyExistsException;
import co.cask.cdap.client.exception.BadRequestException;
import co.cask.cdap.client.exception.CannotBeDeletedException;
import co.cask.cdap.client.exception.NotFoundException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.NamespaceMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Tests for {@link NamespaceClient}
 */
public class NamespaceClientTestRun extends ClientTestBase {
  private NamespaceClient namespaceClient;
  private static final String DOES_NOT_EXIST = "doesnotexist";
  private static final String DEFAULT = "default";
  private static final String SYSTEM = "system";
  private static final String TEST_NAMESPACE_ID = "testnamespace";
  private static final String TEST_DISPLAY_NAME = "testdisplayname";
  private static final String TEST_DESCRIPTION = "testdescription";
  private static final String TEST_DEFAULT_FIELDS = "testdefaultfields";

  @Before
  public void setup() {
    ClientConfig.Builder builder = new ClientConfig.Builder();
    builder.setHostname(HOSTNAME).setPort(PORT);
    namespaceClient = new NamespaceClient(builder.build());
  }

  @Test
  public void testNamespaces() throws IOException, UnAuthorizedAccessTokenException, CannotBeDeletedException,
    NotFoundException, AlreadyExistsException, BadRequestException {
    List<NamespaceMeta> namespaces = namespaceClient.list();
    Assert.assertEquals(0, namespaces.size());

    verifyDoesNotExist(DOES_NOT_EXIST);
    verifyReservedCreate();
    verifyReservedDelete();

    // create a valid namespace
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    builder.setId(TEST_NAMESPACE_ID).setDisplayName(TEST_DISPLAY_NAME).setDescription(TEST_DESCRIPTION);
    namespaceClient.create(builder.build());

    // verify that the namespace got created correctly
    namespaces = namespaceClient.list();
    Assert.assertEquals(1, namespaces.size());
    NamespaceMeta meta = namespaceClient.get(TEST_NAMESPACE_ID);
    Assert.assertEquals(TEST_NAMESPACE_ID, meta.getId());
    Assert.assertEquals(TEST_DISPLAY_NAME, meta.getDisplayName());
    Assert.assertEquals(TEST_DESCRIPTION, meta.getDescription());

    // try creating a namespace with the same id again
    builder.setDisplayName("existing").setDescription("existing");
    try {
      namespaceClient.create(builder.build());
      Assert.fail("Should not be able to re-create an existing namespace");
    } catch (AlreadyExistsException e) {
    }
    // verify that the existing namespace was not updated
    meta = namespaceClient.get(TEST_NAMESPACE_ID);
    Assert.assertEquals(TEST_NAMESPACE_ID, meta.getId());
    Assert.assertEquals(TEST_DISPLAY_NAME, meta.getDisplayName());
    Assert.assertEquals(TEST_DESCRIPTION, meta.getDescription());

    // create and verify namespace without displayName and description
    builder = new NamespaceMeta.Builder();
    builder.setId(TEST_DEFAULT_FIELDS);
    namespaceClient.create(builder.build());
    namespaces = namespaceClient.list();
    Assert.assertEquals(2, namespaces.size());
    meta = namespaceClient.get(TEST_DEFAULT_FIELDS);
    Assert.assertEquals(TEST_DEFAULT_FIELDS, meta.getId());
    Assert.assertEquals(TEST_DEFAULT_FIELDS, meta.getDisplayName());
    Assert.assertEquals("", meta.getDescription());

    // cleanup
    namespaceClient.delete(TEST_NAMESPACE_ID);
    namespaceClient.delete(TEST_DEFAULT_FIELDS);

    Assert.assertEquals(0, namespaceClient.list().size());
  }

  private void verifyDoesNotExist(String namespaceId)
    throws IOException, UnAuthorizedAccessTokenException, CannotBeDeletedException {
    try {
      namespaceClient.get(namespaceId);
      Assert.fail(String.format("Namespace '%s' must not be found", namespaceId));
    } catch (NotFoundException e) {
    }

    try {
      namespaceClient.delete(namespaceId);
      Assert.fail(String.format("Namespace '%s' must not be found", namespaceId));
    } catch (NotFoundException e) {
    }
  }

  private void verifyReservedCreate() throws AlreadyExistsException, IOException, UnAuthorizedAccessTokenException {
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    builder.setId(DEFAULT);
    try {
      namespaceClient.create(builder.build());
      Assert.fail(String.format("Must not create '%s' namespace", DEFAULT));
    } catch (BadRequestException e) {
    }
    builder.setId(SYSTEM);
    try {
      namespaceClient.create(builder.build());
      Assert.fail(String.format("Must not create '%s' namespace", SYSTEM));
    } catch (BadRequestException e) {
    }
  }

  private void verifyReservedDelete() throws NotFoundException, IOException, UnAuthorizedAccessTokenException {
    try {
      namespaceClient.delete(DEFAULT);
      Assert.fail(String.format("Must not delete '%s' namespace", DEFAULT));
    } catch (CannotBeDeletedException e) {
    }
    try {
      namespaceClient.delete(SYSTEM);
      Assert.fail(String.format("Must not delete '%s' namespace", SYSTEM));
    } catch (CannotBeDeletedException e) {
    }
  }
}
