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

package co.cask.cdap.internal.app.runtime.batch.dataset;


import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link MultipleOutputs}.
 */
public class MultipleOutputsTest {

  @Test
  public void testInvalidInputName() throws IOException {
    Job job = Job.getInstance();
    try {
      // the other parameters don't matter, because it fails just checking the name
      MultipleOutputs.addNamedOutput(job, "name.with.dots", null, null, null, null);
      Assert.fail("Expected not to be able to add an output with a '.' in the name");
    } catch (IllegalArgumentException expected) {
      // just check the its not some other IllegalArgumentException that happened
      Assert.assertTrue(expected.getMessage().contains("must consist only of ASCII letters, numbers,"));
    }
  }
}
