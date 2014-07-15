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

package com.continuuity.explore.client;

import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;

/**
 * Explore client discovers explore service, and executes explore commands using the service.
 */
public interface ExploreClient extends Explore {

  /**
   * Returns true if the explore service is up and running.
   */
  boolean isAvailable();

  /**
   * Enables ad-hoc exploration of the given {@link com.continuuity.api.data.batch.RecordScannable}.
   * @param datasetInstance dataset instance name.
   */
  Handle enableExplore(String datasetInstance) throws ExploreException;

  /**
   * Disable ad-hoc exploration of the given {@link com.continuuity.api.data.batch.RecordScannable}.
   * @param datasetInstance dataset instance name.
   */
  Handle disableExplore(String datasetInstance) throws ExploreException;
}
