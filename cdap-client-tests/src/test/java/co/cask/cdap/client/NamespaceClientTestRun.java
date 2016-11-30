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
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link NamespaceClient}
 */
public class NamespaceClientTestRun extends ClientTestBase {
  private NamespaceClient namespaceClient;

  private static final NamespaceId DOES_NOT_EXIST = new NamespaceId("doesnotexist");
  private static final NamespaceId TEST_NAMESPACE_NAME = new NamespaceId("testnamespace");
  private static final String TEST_DESCRIPTION = "testdescription";
  private static final NamespaceId TEST_DEFAULT_FIELDS = new NamespaceId("testdefaultfields");

  @Before
  public void setup() {
    namespaceClient = new NamespaceClient(clientConfig);
  }

  @Test
  public void testNamespaces() throws Exception {
    List<NamespaceMeta> namespaces = namespaceClient.list();
    int initialNamespaceCount = namespaces.size();

    Assert.assertFalse(String.format("Namespace '%s' must not be found", DOES_NOT_EXIST),
                       namespaceClient.exists(DOES_NOT_EXIST));
    verifyReservedCreate();
    verifyReservedDelete();

    // create a valid namespace
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    builder.setName(TEST_NAMESPACE_NAME).setDescription(TEST_DESCRIPTION);
    namespaceClient.create(builder.build());
    waitForNamespaceCreation(TEST_NAMESPACE_NAME);

    // verify that the namespace got created correctly
    namespaces = namespaceClient.list();
    Assert.assertEquals(initialNamespaceCount + 1, namespaces.size());
    NamespaceMeta meta = namespaceClient.get(TEST_NAMESPACE_NAME);
    Assert.assertEquals(TEST_NAMESPACE_NAME.getNamespace(), meta.getName());
    Assert.assertEquals(TEST_DESCRIPTION, meta.getDescription());

    // try creating a namespace with the same id again
    builder.setName(TEST_NAMESPACE_NAME).setDescription("existing");
    try {
      namespaceClient.create(builder.build());
      Assert.fail("Should not be able to re-create an existing namespace");
    } catch (AlreadyExistsException e) {
      // expected
    }
    // verify that the existing namespace was not updated
    meta = namespaceClient.get(TEST_NAMESPACE_NAME);
    Assert.assertEquals(TEST_NAMESPACE_NAME.getNamespace(), meta.getName());
    Assert.assertEquals(TEST_DESCRIPTION, meta.getDescription());

    // create and verify namespace without name and description
    builder = new NamespaceMeta.Builder();
    builder.setName(TEST_DEFAULT_FIELDS);
    namespaceClient.create(builder.build());
    namespaces = namespaceClient.list();
    Assert.assertEquals(initialNamespaceCount + 2, namespaces.size());
    meta = namespaceClient.get(TEST_DEFAULT_FIELDS);
    Assert.assertEquals(TEST_DEFAULT_FIELDS.getNamespace(), meta.getName());
    Assert.assertEquals("", meta.getDescription());

    // cleanup
    namespaceClient.delete(TEST_NAMESPACE_NAME);
    namespaceClient.delete(TEST_DEFAULT_FIELDS);

    Assert.assertEquals(initialNamespaceCount, namespaceClient.list().size());
  }

  @Test
  public void testDeleteAll() throws Exception {
    // create a valid namespace
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    builder.setName(TEST_NAMESPACE_NAME).setDescription(TEST_DESCRIPTION);
    namespaceClient.create(builder.build());
    waitForNamespaceCreation(TEST_NAMESPACE_NAME);

    // create another namespace
    builder = new NamespaceMeta.Builder();
    builder.setName(TEST_DEFAULT_FIELDS);
    namespaceClient.create(builder.build());
    waitForNamespaceCreation(TEST_DEFAULT_FIELDS);

    // cleanup
    namespaceClient.deleteAll();

    // verify that only the default namespace is left
    Assert.assertEquals(1, namespaceClient.list().size());
    Assert.assertEquals(Id.Namespace.DEFAULT.getId(), namespaceClient.list().get(0).getName());
  }

  private void verifyReservedCreate() throws Exception {
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    builder.setName(Id.Namespace.DEFAULT);
    try {
      namespaceClient.create(builder.build());
      Assert.fail(String.format("Must not create '%s' namespace", Id.Namespace.DEFAULT));
    } catch (BadRequestException e) {
      // expected
    }
    builder.setName(Id.Namespace.SYSTEM);
    try {
      namespaceClient.create(builder.build());
      Assert.fail(String.format("Must not create '%s' namespace", Id.Namespace.SYSTEM));
    } catch (BadRequestException e) {
      // expected
    }
  }

  private void verifyReservedDelete() throws Exception {
    // For the purposes of NamespaceClientTestRun, deleting default namespace has no effect.
    // Its lifecycle is already tested in NamespaceHttpHandlerTest
    namespaceClient.delete(NamespaceId.DEFAULT);
    namespaceClient.get(NamespaceId.DEFAULT);
    try {
      namespaceClient.delete(NamespaceId.SYSTEM);
      Assert.fail(String.format("'%s' namespace must not exist", NamespaceId.SYSTEM.getNamespace()));
    } catch (NotFoundException e) {
      // expected
    }
  }

  private void waitForNamespaceCreation(NamespaceId namespace) throws IOException, UnauthenticatedException,
    InterruptedException {
    int count = 0;
    while (count < 10) {
      try {
        namespaceClient.get(namespace);
        return;
      } catch (Throwable t) {
        count++;
        TimeUnit.SECONDS.sleep(1);
      }
    }
  }
}
