package com.continuuity.internal.app.verification;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.app.verification.AbstractVerifier;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.error.Err;

/**
 * This class verifies a {@link DataSetSpecification}.
 * <p>
 * Following are the checks that are done for DataSet.
 * <ul>
 * <li>Check if the dataset name is an id or not</li>
 * </ul>
 * </p>
 */
public class DataSetVerification extends AbstractVerifier<DataSetSpecification> {

  @Override
  protected String getName(DataSetSpecification input) {
    return input.getName();
  }
}
