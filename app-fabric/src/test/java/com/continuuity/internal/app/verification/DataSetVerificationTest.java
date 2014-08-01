/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.verification;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.proto.Id;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests bad dataset specification.
 */
public class DataSetVerificationTest {

  private static final Id.Application DUMMY_APP = Id.Application.from("test", "dummy");

  @Test
  public void testGoodDataSetSpecification() throws Exception {
    DataSetSpecification spec = new DataSetSpecification.Builder(new KeyValueTable("crawl-table"))
      .create();
    DataSetVerification verifier = new DataSetVerification();
    VerifyResult result = verifier.verify(DUMMY_APP, spec);
    Assert.assertTrue(result.getMessage(), result.getStatus() == VerifyResult.Status.SUCCESS);
  }

  @Test
  public void testBadDataSetSpecification() throws Exception {
    DataSetSpecification spec = new DataSetSpecification.Builder(new KeyValueTable("bad dataset"))
                                  .create();
    DataSetVerification verifier = new DataSetVerification();
    VerifyResult result = verifier.verify(DUMMY_APP, spec);
    Assert.assertTrue(result.getMessage(), result.getStatus() == VerifyResult.Status.FAILED);
  }
}
