/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Role;
import com.continuuity.passport.meta.RolesAccounts;
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

  /**
   * Gets the List of VPC for which the account has access to based on the vpcs that the account created
   * and all other vpcs that the account has access.
   * @param accountId accountId of the account
   * @return List of {@code VPC}
   */
  public List<VPC> getVPC(int accountId);

  public VPC getVPC(int accountId, int vpcId);

  public List<VPC> getVPC(String apiKey);

  public int getVPCCount(String vpcName);

  public Account getAccountForVPC(String vpcName);


  /**
   * Get all roles and corresponding accounts for a given vpcName.
   * @param vpcName name of the vpc
   * @return Instance of {@code RolesAccounts}
   */
  public RolesAccounts getRolesAccounts(String vpcName);

  }
