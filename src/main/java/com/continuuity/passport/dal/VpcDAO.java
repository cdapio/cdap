/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.meta.Role;
import com.continuuity.passport.meta.VPC;

import java.util.List;

/**
 *
 */
public interface VpcDAO {

  public VPC addVPC(int accountId, VPC vpc)
    throws ConfigurationException;

  public void removeVPC(int accountId, int vpcId)
    throws ConfigurationException, VPCNotFoundException;

  public boolean addRoles(int accountId, int vpcId, int userId, Role role, String overrides)
    throws ConfigurationException;

  public List<VPC> getVPC(int accountId) throws ConfigurationException;

  public VPC getVPC(int accountId, int vpcId) throws ConfigurationException;

  public List<VPC> getVPC(String apiKey) throws ConfigurationException;

  public int getVPCCount(String vpcName);

}
