/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.filesystem.Location;
import com.google.common.collect.Lists;

import java.util.List;

/**
 *
 */
public final class DeployPipelineData {
  private final String account;
  private final Location archive;
  private ApplicationSpecification specification;
  private List<Location> locations = Lists.newLinkedList();

  public DeployPipelineData(String account, Location archive) {
    this.account = account;
    this.archive = archive;
  }

  public String getAccount() {
    return account;
  }

  public Location getArchive() {
    return archive;
  }

  public ApplicationSpecification getSpecification() {
    return specification;
  }

  public void setSpecification(ApplicationSpecification specification) {
    this.specification = specification;
  }

  public List<Location> getLocations() {
    return locations;
  }

}
