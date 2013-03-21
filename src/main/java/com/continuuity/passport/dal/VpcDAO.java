/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Role;
import com.continuuity.passport.meta.VPC;

import java.util.List;

/**
 *
 */
public interface VpcDAO {

  public VPC addVPC(int accountId, VPC vpc);

  public void removeVPC(String vpcName)throws VPCNotFoundException;

  public void removeVPC(int accountId, int vpcId) throws VPCNotFoundException;

  public boolean addRoles(int accountId, int vpcId, int userId, Role role, String overrides);

  public List<VPC> getVPC(int accountId);

  public VPC getVPC(int accountId, int vpcId);

  public List<VPC> getVPC(String apiKey);

  public int getVPCCount(String vpcName);

  public Account getAccountForVPC(String vpcName);

  }
