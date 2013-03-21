/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.impl;

import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.status.Status;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.NonceDAO;
import com.continuuity.passport.dal.ProfanityFilter;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Component;
import com.continuuity.passport.meta.VPC;
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
  private final ProfanityFilter profanityFilter;

  @Inject
  public DataManagementServiceImpl(AccountDAO accountDAO, VpcDAO vpcDAO,
                                   NonceDAO nonceDAO, ProfanityFilter profanityFilter) {
    this.accountDAO = accountDAO;
    this.vpcDao = vpcDAO;
    this.nonceDAO = nonceDAO;
    this.profanityFilter = profanityFilter;
  }

  /**
   * Register an {@code Account} in the system
   *
   * @param account Account information
   * @return Instance of {@code Status}
   */
  @Override
  public Account registerAccount(Account account) throws AccountAlreadyExistsException {
    return accountDAO.createAccount(account);
  }

  @Override
  public void confirmRegistration(Account account, String password) {
    accountDAO.confirmRegistration(account, password);
  }

  @Override
  public void confirmDownload(int accountId) {
    accountDAO.confirmDownload(accountId);
  }

  @Override
  public void confirmPayment(int accountId, String paymentId) {
    accountDAO.confirmPayment(accountId, paymentId);
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
  public Status registerComponents(String accountId, Credentials credentials, Component component) {
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
    accountDAO.deleteAccount(accountId);
  }

  /**
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   */
  @Override
  public Status updateComponent(String accountId, Credentials credentials, Component component) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * GetAccount object
   *
   * @param accountId Id of the account
   * @return Instance of {@code Account}
   */
  @Override
  public Account getAccount(int accountId) {
    return accountDAO.getAccount(accountId);
  }

  @Override
  public VPC getVPC(int accountId, int vpcId) {
    return vpcDao.getVPC(accountId, vpcId);
  }

  @Override
  public void deleteVPC(int accountId, int vpcId) throws VPCNotFoundException {
    vpcDao.removeVPC(accountId, vpcId);
  }

  @Override
  public void deleteVPC(String vpcName) throws VPCNotFoundException {
    vpcDao.removeVPC(vpcName);
  }

  @Override
  public Account getAccount(String emailId) {
    return accountDAO.getAccount(emailId);
  }

  @Override
  public List<VPC> getVPC(int accountId) {
    return vpcDao.getVPC(accountId);
  }

  /**
   * Get VPC List based on the ApiKey
   *
   * @param apiKey apiKey of the account
   * @return List of {@code VPC}
   */
  @Override
  public List<VPC> getVPC(String apiKey) {
   return vpcDao.getVPC(apiKey);
  }

  /**
   * Update account with passed Params
   *
   * @param accountId accountId
   * @param params    Map<"keyName", "value">
   */
  @Override
  public void updateAccount(int accountId, Map<String, Object> params) {
    accountDAO.updateAccount(accountId, params);
  }

  @Override
  public void changePassword(int accountId, String oldPassword, String newPassword) {
    accountDAO.changePassword(accountId, oldPassword, newPassword);
  }

  /**
   * ResetPassword
   */
  @Override
  public Account resetPassword(int nonceId, String password) {
    return accountDAO.resetPassword(nonceId, password);
  }

  @Override
  public int getActivationNonce(String id) {
    return nonceDAO.getNonce(id, NonceDAO.NONCE_TYPE.ACTIVATION);
  }

  @Override
  public int getSessionNonce(String id) {
    return nonceDAO.getNonce(id, NonceDAO.NONCE_TYPE.SESSION);
  }

  /**
   * Returns if VPC is valid
   * @param vpcName
   * @return
   */
  @Override
  public boolean isValidVPC(String vpcName) {
    return ( ! profanityFilter.isFiltered(vpcName) && vpcDao.getVPCCount(vpcName) == 0 );
  }

  @Override
  public String getActivationId(int nonce) {
    try {
      return nonceDAO.getId(nonce, NonceDAO.NONCE_TYPE.ACTIVATION);
    } catch (StaleNonceException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String getSessionId(int nonce) {

    try {
      return nonceDAO.getId(nonce, NonceDAO.NONCE_TYPE.SESSION);
    } catch (StaleNonceException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Generate Reset Nonce
   *
   * @param id
   * @return random nonce
   */
  @Override
  public int getResetNonce(String id) {
    return nonceDAO.getNonce(id, NonceDAO.NONCE_TYPE.RESET);
  }

  /**
   * Regenerate API Key
   *
   * @param accountId
   */
  @Override
  public void regenerateApiKey(int accountId) {
    accountDAO.regenerateApiKey(accountId);
  }


  public VPC addVPC(int accountId, VPC vpc) {
    if ( ! profanityFilter.isFiltered(vpc.getVpcName())) {
       return vpcDao.addVPC(accountId, vpc);
    }
    return null;
  }

  @Override
  public Account getAccountForVPC(String vpcName) {
    return vpcDao.getAccountForVPC(vpcName);
  }
}
