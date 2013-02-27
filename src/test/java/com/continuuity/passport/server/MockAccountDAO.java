package com.continuuity.passport.server;

import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.BillingInfo;
import com.continuuity.passport.meta.Role;

import java.util.HashMap;
import java.util.Map;

/**
 * MockAccountDAO class that is used for testing.
 *  Note: TODO: This is not fully implemented. Initally used to test passport client.
 */
public class MockAccountDAO implements AccountDAO {

  /**
   * Create Account in the system
   *
   * @param account Instance of {@code Account}
   * @return int account Id that was generated
   * @throws {@code RetryException}
   */
  private static  Map<Integer, Account> accountHash = new HashMap<Integer, Account>() ;
  private static  Map<String, Account> accountByApiKeyHash = new HashMap<String,Account>();
  //simulates auto-increment
  private int nextAccountId = 0;


  @Override
  public Account createAccount(Account account) throws ConfigurationException, AccountAlreadyExistsException {


    nextAccountId++;
    Integer accountId = nextAccountId;
    Account returnAccount  = new Account(account.getFirstName(),account.getLastName(),
                             account.getCompany(),account.getEmailId(),accountId.intValue());
    accountHash.put(accountId,returnAccount);
    String apiKey = String.format("apiKey%s",accountId);
    accountByApiKeyHash.put(apiKey,returnAccount);
    return returnAccount;
  }

  @Override
  public boolean confirmRegistration(Account account, String password) throws ConfigurationException {

    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * @param accountId
   * @throws com.continuuity.passport.core.exceptions.ConfigurationException
   *
   * @throws RuntimeException
   */
  @Override
  public void confirmDownload(int accountId) throws ConfigurationException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Delete Account in the system
   *
   * @param accountId AccountId to be deleted
   * @return boolean status of account deletion
   * @throws {@code RetryException}
   */
  @Override
  public boolean deleteAccount(int accountId) throws ConfigurationException, RuntimeException, AccountNotFoundException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * GetAccount
   *
   * @param accountId AccountId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  @Override
  public Account getAccount(int accountId) throws ConfigurationException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * GetAccount
   *
   * @param emailId emailId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  @Override
  public Account getAccount(String emailId) throws ConfigurationException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean updateBillingInfo(int accountId, BillingInfo billingInfo) throws ConfigurationException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean addRoleType(int accountId, Role role) throws ConfigurationException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void updateAccount(int accountId, Map<String, Object> keyValueParams) throws ConfigurationException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void changePassword(int accountId, String oldPassword, String newPassword) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Account resetPassword(int nonce, String newPassword) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void regenerateApiKey(int accountId) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public static Account authenticate(String apiKey) {
    return (accountByApiKeyHash.get(apiKey));
  }
}
