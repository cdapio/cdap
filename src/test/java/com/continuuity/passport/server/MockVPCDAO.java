package com.continuuity.passport.server;

import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Role;
import com.continuuity.passport.meta.VPC;

import java.util.List;

/**
 *  Mock VPC DAO object used for testing. Stores data in a hashMap and returns them back.
 *  Note: TODO: This is not fully implemented. Initally used to test passport client.
 */
public class MockVPCDAO implements VpcDAO {

  @Override
  public VPC addVPC(int accountId, VPC vpc) throws ConfigurationException {
    return null;  
  }

  @Override
  public void removeVPC(String vpcName) throws VPCNotFoundException {
  }

  @Override
  public void removeVPC(int accountId, int vpcId) throws ConfigurationException, VPCNotFoundException {
    
  }

  @Override
  public boolean addRoles(int accountId, int vpcId, int userId, Role role, String overrides) throws ConfigurationException {
    return false;  
  }

  @Override
  public List<VPC> getVPC(int accountId) throws ConfigurationException {
    return null;  
  }

  @Override
  public VPC getVPC(int accountId, int vpcId) throws ConfigurationException {
    return null;  
  }

  @Override
  public List<VPC> getVPC(String apiKey) throws ConfigurationException {
    return null;  
  }

  @Override
  public int getVPCCount(String vpcName) {
    return 0;  
  }

  @Override
  public Account getAccountForVPC(String vpcName) {
    return null;  
  }
}
