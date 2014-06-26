package com.continuuity.internal.app.verification;

import com.continuuity.app.verification.AbstractVerifier;
import com.continuuity.data.dataset.DatasetCreationSpec;

/**
 * This class verifies a {@link com.continuuity.data.dataset.DatasetCreationSpec}.
 * <p>
 * Following are the checks that are done.
 * <ul>
 * <li>Check if the dataset name is an id or not</li>
 * </ul>
 * </p>
 */
public class DatasetCreationSpecVerifier extends AbstractVerifier<DatasetCreationSpec> {

  @Override
  protected String getName(DatasetCreationSpec spec) {
    return spec.getInstanceName();
  }
}
