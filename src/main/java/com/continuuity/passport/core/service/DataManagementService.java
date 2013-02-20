package com.continuuity.passport.core.service;


import com.continuuity.passport.core.exceptions.*;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.Component;
import com.continuuity.passport.core.meta.VPC;
import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.core.status.Status;

import java.util.List;
import java.util.Map;

/**
 *  Service that orchestrates all account and vpc crud operations
 */
public interface DataManagementService {

  /**
   * Register an {@code Account} in the system. Updates underlying data stores. Generates a unique account Id
   * @param account Account information
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Account registerAccount(Account account) throws RuntimeException, AccountAlreadyExistsException;

  /**
   * Delete an {@code Account} in the system
   * @param accountId accountId to be deleted
   * @throws RuntimeException on any errors from underlying data access objects
   * @throws AccountNotFoundException on account to be deleted not found
   */
  public void deleteAccount(int accountId) throws RuntimeException, AccountNotFoundException;

  /**
   * Confirms the registration, generates API Key
   * @param account   Instance of {@code Account}
   * @param password  Password to be stored
   * @return  Instance on {@code Status}
   * @throws RuntimeException on any errors from underlying data access objects
   */
  public Status confirmRegistration(Account account, String password) throws RuntimeException;

  /**
   * Register the fact that the user has downloaded the Dev suite
   * @param accountId  accountId that downloaded dev suite
   * @throws RuntimeException   on any errors from underlying data access objects
   */
  public void confirmDownload(int accountId) throws RuntimeException;


  /**
   * GetAccount object
   *
   * @param accountId lookup account Id of the account
   * @return Instance of {@code Account}
   * @throws RuntimeException on un-recoverable errors from Data access objects
   */
  public Account getAccount(int accountId) throws RuntimeException;


  /**
   * Get Account object from the system
   * @param emailId look up by emailId
   * @return  Instance of {@code Account}
   * @throws RuntimeException   on un-recoverable errors from Data access objects
   */
  public Account getAccount(String emailId) throws RuntimeException;

  /**
   * Update account with passed Params
   *
   * @param accountId accountId
   * @param params    Map<"keyName", "value">
   */
  public void updateAccount(int accountId, Map<String, Object> params) throws RuntimeException;

  /**
   * Change password for account
   * @param accountId  accountId
   * @param oldPassword old password in the system
   * @param newPassword new password in the system
   * @throws RuntimeException on any errors from underlying data access objects
   */
  public void changePassword(int accountId, String oldPassword, String newPassword) throws RuntimeException;


  /**
   * Add Meta-data for VPC, updates underlying data stores and generates a VPC ID.
   * TODO: Checks for profanity keywords in vpc name and labels
   * @param accountId
   * @param vpc
   * @return Instance of {@code VPC}
   * @throws RuntimeException
   */
  public VPC addVPC(int accountId, VPC vpc) throws RuntimeException;

  /**
   * Get VPC - lookup by accountId and VPCID
   * @param accountId
   * @param vpcID
   * @return Instance of {@code VPC}
   */
  public VPC getVPC(int accountId, int vpcID);

  /**
   * Delete VPC
   * @param accountId
   * @param vpcId
   * @throws RuntimeException
   * @throws VPCNotFoundException when VPC is not present in underlying data stores
   */
  public void deleteVPC(int accountId, int vpcId) throws RuntimeException, VPCNotFoundException;


  /**
   * Get VPC list for accountID
   *
   * @param accountId accountId identifying accounts
   * @return List of {@code VPC}
   */
  public List<VPC> getVPC(int accountId);

  /**
   * Get VPC List based on the ApiKey
   *
   * @param apiKey apiKey of the account
   * @return List of {@code VPC}
   */
  public List<VPC> getVPC(String apiKey);


  /**
   * Generate a unique id to be used in activation email to enable secure (nonce based) registration process.
   * @param id accountID to be nonce
   * @return random nonce
   * @throws RuntimeException on failure to generate and store nonce
   * TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  public int getActivationNonce(int id) throws RuntimeException;

  /**
   * Get id for nonce
   * @param nonce  nonce that was generated.
   * @return id
   * @throws RuntimeException
   * @throws StaleNonceException on nonce that was generated expiring in the system
   * TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  public int getActivationId(int nonce) throws RuntimeException, StaleNonceException;

  /**
   * Generate a nonce that will be used for sessions.
   * @param id  accountId to be nonced
   * @return  random nonce
   * @throws RuntimeException on failure to generate and store nonce
   * TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  public int getSessionNonce(int id) throws RuntimeException;

  /**
   * Get id for nonce
   * @param nonce
   * @return account id for nonce key
   * @throws RuntimeException
   * @throws StaleNonceException on nonce that was generated expiring in the system
   * TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  public int getSessionId(int nonce) throws RuntimeException, StaleNonceException;

  /**
   * Register a component with the account- Example: register VPC, Register DataSet
   * TODO: Note: This is not implemented for initial free VPC use case
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status registerComponents(String accountId, Credentials credentials,
                                   Component component) throws RetryException;

  /**
   * Unregister a {@code Component} in the system
   * TODO: Note: This is not implemented for initial free VPC use case
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status unRegisterComponent(String accountId, Credentials credentials,
                                    Component component) throws RetryException;


  /**
   * TODO: Note: This is not implemented for initial free VPC use case
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status updateComponent(String accountId, Credentials credentials, Component component) throws RetryException;


}
