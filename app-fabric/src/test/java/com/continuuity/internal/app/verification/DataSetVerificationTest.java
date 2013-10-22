/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.verification;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.app.Id;
import com.continuuity.app.verification.VerifyResult;
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
