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

package io.cdap.cdap.common.security;

import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ParentedId;
import io.cdap.cdap.proto.id.ProgramId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AuthEnforceUtil}.
 */
public class AuthEnforceUtilTest {
  private class ExtAppId extends ApplicationId {
    public ExtAppId(String namespace, String application) {
      super(namespace, application);
    }
  }

  private class ExtAppIdChild implements ParentedId<ExtAppId> {
    @Override
    public ExtAppId getParent() {
      return null;
    }
  }

  @Test
  public void testSameClassSuccess() {
    Assert.assertTrue(AuthEnforceUtil.verifyEntityIdParents(NamespaceId.class, NamespaceId.class));
  }

  @Test
  public void testClassIsChildSuccess() {
    Assert.assertTrue(AuthEnforceUtil.verifyEntityIdParents(ApplicationId.class, NamespaceId.class));
  }

  @Test
  public void testSuperclassIsChildSuccess() {
    Assert.assertTrue(AuthEnforceUtil.verifyEntityIdParents(ExtAppId.class, NamespaceId.class));
  }

  @Test
  public void testChildOfChildSuccess() {
    Assert.assertTrue(AuthEnforceUtil.verifyEntityIdParents(ArtifactId.class, NamespaceId.class));
  }

  @Test
  public void testParentIsSubclassOfEnforceOnSuccess() {
    Assert.assertTrue(AuthEnforceUtil.verifyEntityIdParents(ExtAppIdChild.class, NamespaceId.class));
  }

  @Test
  public void testClassIsNotChildFail() {
    Assert.assertFalse(AuthEnforceUtil.verifyEntityIdParents(Integer.class, NamespaceId.class));
  }

  @Test
  public void testClassWithParentsIsNotChildFail() {
    Assert.assertFalse(AuthEnforceUtil.verifyEntityIdParents(ArtifactId.class, ProgramId.class));
  }
}
