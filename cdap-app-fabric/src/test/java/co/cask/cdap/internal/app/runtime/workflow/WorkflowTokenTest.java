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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.workflow.WorkflowToken;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link WorkflowToken}
 */
public class WorkflowTokenTest {

  @Test
  public void testNonUpdatableWorkflowToken() {
    BasicWorkflowToken token = new BasicWorkflowToken(0);
    token.setCurrentNode("node");
    try {
      token.put("a", "b");
      Assert.fail("Workflow token update should fail because the token is non-updatable.");
    } catch (IllegalStateException e) {
      assertSizeExceededErrorMessage(e);
    }
  }

  @Test
  public void testUpdateWithLargeRecord() {
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    token.setCurrentNode("node");
    try {
      token.put("k", generateDataInKb(1024));
      Assert.fail("Workflow token update should fail because token size should have exceeded limit.");
    } catch (IllegalStateException e) {
      assertSizeExceededErrorMessage(e);
    }
  }

  @Test
  public void testMergeLargeWorkflowToken() {
    BasicWorkflowToken token1 = new BasicWorkflowToken(1);
    token1.setCurrentNode("node1");
    // total size of token1 after this operation is 512KB
    token1.put(generateDataInKb(1), generateDataInKb(511));
    // add an additional 2 bytes, just so we have size > max size (and not equal to max size) after merge
    token1.put("k", "v");
    BasicWorkflowToken token2 = new BasicWorkflowToken(1);
    token2.setCurrentNode("node1");
    // total size of token2 after this operation is 512KB
    token2.put(generateDataInKb(1), generateDataInKb(511));
    // should succeed, because token1 already contains the NodeValue being merged
    token1.mergeToken(token2);
    // set a different node in token2 and add the same data
    token2.setCurrentNode("node2");
    // token2 is at capacity after the following operation
    token2.put(generateDataInKb(1), generateDataInKb(511));
    // merging should now fail, because token1 now does not contain the NodeValue being merged
    try {
      token1.mergeToken(token2);
      Assert.fail("Workflow token merging should fail because token size should have exceeded limit.");
    } catch (IllegalStateException e) {
      assertSizeExceededErrorMessage(e);
    }
  }

  @Test
  public void testRepeatedPutAtSameNode() {
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    token.setCurrentNode("node1");
    // after this put, size would be 512KB
    token.put(generateDataInKb(1), generateDataInKb(511));
    // after another successful put at a different node, size would be 1024KB. Workflow token would be at capacity.
    token.setCurrentNode("node2");
    token.put(generateDataInKb(1), generateDataInKb(511));
    // should succeed because the entry at key k1 should be replaced
    token.put(generateDataInKb(1), generateDataInKb(511));
    // now should fail, because even though we're updating node2's value, we're adding an extra KB
    try {
      token.put(generateDataInKb(1), generateDataInKb(512));
      Assert.fail("Workflow token update at existing key should fail because token size should have exceeded limit.");
    } catch (IllegalStateException e) {
      assertSizeExceededErrorMessage(e);
    }
  }

  private String generateDataInKb(int kb) {
    int bytes = kb * 1024;
    StringBuilder sb = new StringBuilder(kb);
    for (int i = 0; i < bytes; i++) {
      sb.append("a");
    }
    return sb.toString();
  }

  private void assertSizeExceededErrorMessage(IllegalStateException e) {
    Assert.assertTrue(e.getMessage().startsWith("Exceeded maximum permitted size of workflow token"));
  }
}
