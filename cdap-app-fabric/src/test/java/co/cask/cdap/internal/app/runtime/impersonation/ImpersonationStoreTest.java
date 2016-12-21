/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.impersonation;

import co.cask.cdap.common.ImpersonationInfoNotFound;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.security.ImpersonationInfo;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Test cases for {@link ImpersonationStore}.
 */
public class ImpersonationStoreTest {

  private static ImpersonationStore impersonationStore;

  @BeforeClass
  public static void setup() throws Exception {
    impersonationStore = AppFabricTestHelper.getInjector().getInstance(ImpersonationStore.class);
  }

  @After
  public void cleanup() throws Exception {
    impersonationStore.clear();
  }

  @Test
  public void testImpersonationInfos() throws Exception {
    // no artifacts in a namespace should return an empty collection
    Assert.assertTrue(impersonationStore.listImpersonationInfos().isEmpty());

    ImpersonationInfo impersonationInfo1 = new ImpersonationInfo("somePrincipal", "/some/path");

    try {
      impersonationStore.getImpersonationInfo(impersonationInfo1.getPrincipal());
      Assert.fail();
    } catch (ImpersonationInfoNotFound e) {
      // expected
    }
    // delete is idempotent, so won't throw NotFoundException
    impersonationStore.delete(impersonationInfo1.getPrincipal());

    impersonationStore.addImpersonationInfo(impersonationInfo1);
    Assert.assertEquals(impersonationInfo1, impersonationStore.getImpersonationInfo("somePrincipal"));

    Set<ImpersonationInfo> expected = ImmutableSet.of(impersonationInfo1);
    Assert.assertEquals(expected, new HashSet<>(impersonationStore.listImpersonationInfos()));


    ImpersonationInfo impersonationInfo2 = new ImpersonationInfo("otherPrinc", "/some/other/path");
    ImpersonationInfo impersonationInfo3 = new ImpersonationInfo("yetAnotherPrincipal", "/different/path");

    impersonationStore.addImpersonationInfo(impersonationInfo2);
    impersonationStore.addImpersonationInfo(impersonationInfo3);
    expected = ImmutableSet.of(impersonationInfo1, impersonationInfo2, impersonationInfo3);
    Assert.assertEquals(expected, new HashSet<>(impersonationStore.listImpersonationInfos()));

    impersonationStore.delete(impersonationInfo1.getPrincipal());
    expected = ImmutableSet.of(impersonationInfo2, impersonationInfo3);
    Assert.assertEquals(expected, new HashSet<>(impersonationStore.listImpersonationInfos()));
  }

  @Test
  public void testAssociations() throws Exception {
    StreamId streamId = NamespaceId.DEFAULT.stream("fooStream");

    try {
      impersonationStore.getAssociation(streamId);
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }

    // delete behavior is idempotent, so won't throw NotFoundException
    impersonationStore.deleteAssociation(streamId);

    ImpersonationInfo impersonationInfo = new ImpersonationInfo("somePrincipal", "/some/path");

    try {
      // this should fail, because the principal has no ImpersonationInfo
      impersonationStore.createAssociation(streamId, impersonationInfo.getPrincipal());
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    impersonationStore.addImpersonationInfo(impersonationInfo);
    // add another impersonation info, just so the above is not the only impersonation info added
    impersonationStore.addImpersonationInfo(new ImpersonationInfo("otherPrinc", "/some/other/path"));

    // now that we added the impersonation info, creating an association should be fine
    impersonationStore.createAssociation(streamId, impersonationInfo.getPrincipal());

    Assert.assertEquals(impersonationInfo, impersonationStore.getAssociation(streamId));


    // TODO: this should fail because there is an association for this impersonation info
//    impersonationStore.delete(impersonationInfo.getPrincipal());

    impersonationStore.deleteAssociation(streamId);
    try {
      impersonationStore.getAssociation(streamId);
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }

    // now, we should be able to delete the impersonation info
    impersonationStore.delete(impersonationInfo.getPrincipal());
  }
}
