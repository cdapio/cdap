package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.core.meta.Role;
import com.continuuity.passport.core.meta.VPC;

import java.util.List;

/**
 *
 */
public interface VpcDAO {

  public VPC addVPC ( int accountId, VPC vpc)
                          throws ConfigurationException, RuntimeException;

  public void removeVPC (int accountId, int vpcId)
                          throws ConfigurationException, RuntimeException, VPCNotFoundException;

  public boolean addRoles (int accountId, int vpcId, int userId, Role role, String overrides )
                          throws ConfigurationException, RuntimeException;

  public List<VPC> getVPC(int accountId) throws RuntimeException,ConfigurationException;

  public   VPC getVPC(int accountId, int vpcId) throws RuntimeException,ConfigurationException;


  public List<VPC> getVPC(String apiKey) throws RuntimeException,ConfigurationException;

}
