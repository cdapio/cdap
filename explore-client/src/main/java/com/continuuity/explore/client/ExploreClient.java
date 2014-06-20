package com.continuuity.explore.client;

import com.continuuity.api.data.batch.RowScannable;
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
  boolean isAvailable() throws ExploreException;

  /**
   * Enables ad-hoc exploration of the given {@link RowScannable}.
   * @param datasetInstance dataset instance name.
   */
  Handle enableExplore(String datasetInstance) throws ExploreException;

  /**
   * Disable ad-hoc exploration of the given {@link RowScannable}.
   * @param datasetInstance dataset instance name.
   */
  Handle disableExplore(String datasetInstance) throws ExploreException;
}
