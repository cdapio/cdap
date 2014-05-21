package com.continuuity.internal.app.verification;

import com.continuuity.api.data.DatasetInstanceCreationSpec;
import com.continuuity.app.verification.AbstractVerifier;

/**
 * This class verifies a {@link com.continuuity.api.data.DatasetInstanceCreationSpec}.
 * <p>
 * Following are the checks that are done.
 * <ul>
 * <li>Check if the dataset name is an id or not</li>
 * </ul>
 * </p>
 */
public class DatasetInstanceCreationSpecVerifier extends AbstractVerifier<DatasetInstanceCreationSpec> {

  @Override
  protected String getName(DatasetInstanceCreationSpec spec) {
    return spec.getInstanceName();
  }
}
