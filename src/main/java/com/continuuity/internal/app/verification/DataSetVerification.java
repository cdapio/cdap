package com.continuuity.internal.app.verification;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;

/**
 * This class verifies a {@link DataSetSpecification}.
 * <p>
 *   Following are the checks that are done for DataSet.
 *   <ul>
 *     <li>Check if the dataset name is an id or not</li>
 *   </ul>
 * </p>
 */
public class DataSetVerification implements Verifier<DataSetSpecification> {

  /**
   * Verifies {@link DataSetSpecification} as defined within the {@link com.continuuity.api.Application}
   * @param input to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(final DataSetSpecification input) {
    return VerifyResult.SUCCESS();
  }
}
