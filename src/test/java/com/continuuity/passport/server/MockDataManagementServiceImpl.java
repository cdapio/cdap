package com.continuuity.passport.server;

import com.continuuity.passport.core.exceptions.*;
import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.status.Status;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.NonceDAO;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Component;
import com.continuuity.passport.meta.VPC;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class MockDataManagementServiceImpl implements DataManagementService {


  private final AccountDAO accountDAO;
  private final VpcDAO vpcDao;
  private final NonceDAO nonceDAO;

  @Inject
  public MockDataManagementServiceImpl(AccountDAO accountDAO, VpcDAO vpcDAO, NonceDAO nonceDAO) {
    this.accountDAO = accountDAO;
    this.vpcDao = vpcDAO;
    this.nonceDAO = nonceDAO;
  }


  @Override
  public Account registerAccount(Account account) throws AccountAlreadyExistsException {
    Account accountCreated = null;
    try {
      accountCreated = accountDAO.createAccount(account);
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
    return accountCreated;
  }

  /**
   * Delete an {@code Account} in the system
   *
   * @param accountId accountId to be deleted
   * @throws com.continuuity.passport.core.exceptions.AccountNotFoundException
   *          on account to be deleted not found
   */
  @Override
  public void deleteAccount(int accountId) throws AccountNotFoundException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Confirms the registration, generates API Key
   *
   * @param account  Instance of {@code Account}
   * @param password Password to be stored
   */
  @Override
  public void confirmRegistration(Account account, String password) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Register the fact that the user has downloaded the Dev suite
   *
   * @param accountId accountId that downloaded dev suite
   */
  @Override
  public void confirmDownload(int accountId) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * GetAccount object
   *
   * @param accountId lookup account Id of the account
   * @return Instance of {@code Account}
   */
  @Override
  public Account getAccount(int accountId) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get Account object from the system
   *
   * @param emailId look up by emailId
   * @return Instance of {@code Account}
   */
  @Override
  public Account getAccount(String emailId) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Update account with passed Params
   *
   * @param accountId accountId
   * @param params    Map<"keyName", "value">
   */
  @Override
  public void updateAccount(int accountId, Map<String, Object> params) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Change password for account
   *
   * @param accountId   accountId
   * @param oldPassword old password in the system
   * @param newPassword new password in the system
   */
  @Override
  public void changePassword(int accountId, String oldPassword, String newPassword) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * ResetPassword
   */
  @Override
  public Account resetPassword(int nonceId, String password) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Add Meta-data for VPC, updates underlying data stores and generates a VPC ID.
   * TODO: Checks for profanity keywords in vpc name and labels
   *
   * @param accountId
   * @param vpc
   * @return Instance of {@code VPC}
   */
  @Override
  public VPC addVPC(int accountId, VPC vpc) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get VPC - lookup by accountId and VPCID
   *
   * @param accountId
   * @param vpcID
   * @return Instance of {@code VPC}
   */
  @Override
  public VPC getVPC(int accountId, int vpcID) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Delete VPC
   *
   * @param accountId
   * @param vpcId
   * @throws com.continuuity.passport.core.exceptions.VPCNotFoundException
   *          when VPC is not present in underlying data stores
   */
  @Override
  public void deleteVPC(int accountId, int vpcId) throws VPCNotFoundException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get VPC list for accountID
   *
   * @param accountId accountId identifying accounts
   * @return List of {@code VPC}
   */
  @Override
  public List<VPC> getVPC(int accountId) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get VPC List based on the ApiKey
   *
   * @param apiKey apiKey of the account
   * @return List of {@code VPC}
   */
  @Override
  public List<VPC> getVPC(String apiKey) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Generate a unique id to be used in activation email to enable secure (nonce based) registration process.
   *
   * @param id Id to be nonced
   * @return random nonce
   *         TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  @Override
  public int getActivationNonce(String id) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get id for nonce
   *
   * @param nonce nonce that was generated.
   * @return id
   * @throws com.continuuity.passport.core.exceptions.StaleNonceException
   *          on nonce that was generated expiring in the system
   *          TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  @Override
  public String getActivationId(int nonce) throws StaleNonceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Generate a nonce that will be used for sessions.
   *
   * @param id ID to be nonced
   * @return random nonce
   *         TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  @Override
  public int getSessionNonce(String id) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * VPC count for the vpc
   *
   * @param vpcName
   * @return
   */
  @Override
  public int getVPCCount(String vpcName) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get id for nonce
   *
   * @param nonce
   * @return account id for nonce key
   * @throws com.continuuity.passport.core.exceptions.StaleNonceException
   *          on nonce that was generated expiring in the system
   *          TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  @Override
  public String getSessionId(int nonce) throws StaleNonceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Generate Reset Nonce
   *
   * @param id
   * @return random nonce
   */
  @Override
  public int getResetNonce(String id) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Regenerate API Key
   *
   * @param accountId
   */
  @Override
  public void regenerateApiKey(int accountId) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * GetAccount given a VPC name
   *
   * @param vpcName
   * @return
   */
  @Override
  public Account getAccountForVPC(String vpcName) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Register a component with the account- Example: register VPC, Register DataSet
   * TODO: Note: This is not implemented for initial free VPC use case
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
   * TODO: Note: This is not implemented for initial free VPC use case
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
   * TODO: Note: This is not implemented for initial free VPC use case
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   */
  @Override
  public Status updateComponent(String accountId, Credentials credentials, Component component) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
