/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.impl;

import com.continuuity.passport.core.exceptions.*;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Component;
import com.continuuity.passport.meta.VPC;
import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.status.Status;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.NonceDAO;
import com.continuuity.passport.dal.VpcDAO;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

/**
 * Implementation for Data-management service
 */
public class DataManagementServiceImpl implements DataManagementService {
  private final AccountDAO accountDAO;
  private final VpcDAO vpcDao;
  private final NonceDAO nonceDAO;

  @Inject
  public DataManagementServiceImpl(AccountDAO accountDAO, VpcDAO vpcDAO, NonceDAO nonceDAO) {
    this.accountDAO = accountDAO;
    this.vpcDao = vpcDAO;
    this.nonceDAO = nonceDAO;
  }

  /**
   * Register an {@code Account} in the system
   *
   * @param account Account information
   * @return Instance of {@code Status}
   */
  @Override
  public Account registerAccount(Account account) throws AccountAlreadyExistsException {
    Preconditions.checkNotNull(accountDAO,"Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO, "Nonce data access objects cannot be null");
    Account accountCreated = null;
    try {
      accountCreated = accountDAO.createAccount(account);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
    return accountCreated;
  }

  @Override
  public void confirmRegistration(Account account, String password) {
    Preconditions.checkNotNull(accountDAO, "Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO,"Nonce data access objects cannot be null");

    try {
      accountDAO.confirmRegistration(account, password);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void confirmDownload(int accountId){
    Preconditions.checkNotNull(accountDAO,"Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO, "Nonce data access objects cannot be null");

    try {
      accountDAO.confirmDownload(accountId);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Register a component with the account- Example: register VPC, Register DataSet
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   */
  @Override
  public Status registerComponents(String accountId, Credentials credentials, Component component){
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Unregister a {@code Component} in the system
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   */
  @Override
  public Status unRegisterComponent(String accountId, Credentials credentials, Component component) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Delete an {@code Account} in the system
   *
   * @param accountId account to be deleted
   */
  @Override
  public void deleteAccount(int accountId) throws AccountNotFoundException {
    Preconditions.checkNotNull(accountDAO,"Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO, "Nonce data access objects cannot be null");

    try {
      accountDAO.deleteAccount(accountId);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   */
  @Override
  public Status updateComponent(String accountId, Credentials credentials, Component component){
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * GetAccount object
   *
   * @param accountId Id of the account
   * @return Instance of {@code Account}
   */
  @Override
  public Account getAccount(int accountId)  {
    Preconditions.checkNotNull(accountDAO, "Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO,"Nonce data access objects cannot be null");

    Account account = null;
    try {
      account = accountDAO.getAccount(accountId);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
    return account;
  }

  @Override
  public VPC getVPC(int accountId, int vpcId) {
    Preconditions.checkNotNull(accountDAO,"Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO,"Nonce data access objects cannot be null");
    VPC vpc = null;
    try {
      vpc  =  vpcDao.getVPC(accountId, vpcId);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
    return vpc;
  }

  @Override
  public void deleteVPC(int accountId, int vpcId) throws VPCNotFoundException {
    Preconditions.checkNotNull(accountDAO,"Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO, "Nonce data access objects cannot be null");
    try {
      vpcDao.removeVPC(accountId, vpcId);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }


  }

  @Override
  public Account getAccount(String emailId){
    Preconditions.checkNotNull(accountDAO, "Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO,"Nonce data access objects cannot be null");

    Account account = null;

    try {
      account = accountDAO.getAccount(emailId);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
    return account;
  }

  @Override
  public List<VPC> getVPC(int accountId) {

    Preconditions.checkNotNull(accountDAO, "Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO,"Nonce data access objects cannot be null");

    List<VPC> vpcs = null;

    try {
      vpcs = vpcDao.getVPC(accountId);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
    return vpcs;
  }

  /**
   * Get VPC List based on the ApiKey
   *
   * @param apiKey apiKey of the account
   * @return List of {@code VPC}
   */
  @Override
  public List<VPC> getVPC(String apiKey) {
    Preconditions.checkNotNull(accountDAO,"Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO,"Nonce data access objects cannot be null");

    List<VPC> vpcs = null;
    try {
      vpcs = vpcDao.getVPC(apiKey);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
    return vpcs;
 }

  /**
   * Update account with passed Params
   *
   * @param accountId accountId
   * @param params    Map<"keyName", "value">
   */
  @Override
  public void updateAccount(int accountId, Map<String, Object> params) {
    Preconditions.checkNotNull(accountDAO,"Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO,"Nonce data access objects cannot be null");

    try {
      accountDAO.updateAccount(accountId, params);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void changePassword(int accountId, String oldPassword, String newPassword) {
    Preconditions.checkNotNull(accountDAO, "Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO,"Nonce data access objects cannot be null");

    try {
      accountDAO.changePassword(accountId, oldPassword, newPassword);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }

  @Override
  public int getActivationNonce(int id){
    Preconditions.checkNotNull(accountDAO, "Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO,"Nonce data access objects cannot be null");
    int nonce= -1;
    try {
      nonce =  nonceDAO.getNonce(id, NonceDAO.NONCE_TYPE.ACTIVATION);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return nonce;
  }

  @Override
  public int getSessionNonce(int id) {
    Preconditions.checkNotNull(accountDAO,"Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO, "Nonce data access objects cannot be null");
    int nonce = -1 ;
    try {
      nonce =  nonceDAO.getNonce(id, NonceDAO.NONCE_TYPE.SESSION);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return nonce;
  }

  @Override
  public int getActivationId(int nonce) {
    Preconditions.checkNotNull(accountDAO,"Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO, "Nonce data access objects cannot be null");
    int id = -1;
    try {
      id = nonceDAO.getId(nonce, NonceDAO.NONCE_TYPE.ACTIVATION);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return id;
  }

  @Override
  public int getSessionId(int nonce) {
    Preconditions.checkNotNull(accountDAO,"Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO, "Nonce data access objects cannot be null");

    int id = -1;
    try {
      id =  nonceDAO.getId(nonce, NonceDAO.NONCE_TYPE.SESSION);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return id;
  }

  public VPC addVPC(int accountId, VPC vpc) {
    Preconditions.checkNotNull(accountDAO, "Account data access objects cannot be null");
    Preconditions.checkNotNull(vpcDao,"VPC data access objects cannot be null");
    Preconditions.checkNotNull(nonceDAO,"Nonce data access objects cannot be null");

    VPC vpcReturned = null;
    try {
      vpcReturned =  vpcDao.addVPC(accountId, vpc);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
    return vpcReturned;
  }
}
