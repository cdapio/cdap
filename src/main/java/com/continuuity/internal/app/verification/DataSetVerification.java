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
public class DataSetVerification extends AbstractVerifier implements Verifier<DataSetSpecification> {

  /**
   * Verifies {@link DataSetSpecification} as defined within the {@link com.continuuity.api.Application}
   *
   * @param input to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(final DataSetSpecification input) {
    // Checks if DataSet name is an ID
    if(!isId(input.getName())) {
      return VerifyResult.FAILURE(Err.NOT_AN_ID, "Dataset");
    }
    return VerifyResult.SUCCESS();
  }
}
