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
