/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.google.inject.Inject;

import java.io.IOException;

public class DeployableStorage {
  private final Location baseDir;
  private final LocationFactory locationFactory;

  @Inject
  public DeployableStorage(final LocationFactory locationFactory, final CConfiguration configuration) {
    this.locationFactory = locationFactory;

    baseDir = locationFactory.create(configuration.get(
                                          Constants.CFG_RESOURCE_MANAGER_REMOTE_DIR,
                                          Constants.DEFAULT_RESOURCE_MANAGER_REMOTE_DIR) + "/resource");
  }

  public Location getNewLocation(final String localFileName, final ResourceIdentifier identifier) throws IOException {
    String filename = localFileName;
    Location dir = locationFactory.create(baseDir.toURI().getPath() + "/" + identifier.getResource() +
                                              "/" + identifier.getVersion());
    dir.mkdirs();
    if(!localFileName.contains(Constants.JAR_EXTENSION)) {
      filename = localFileName + Constants.JAR_EXTENSION;
    }

    return locationFactory.create(baseDir.toURI().getPath() + "/" + filename);
  }
}
