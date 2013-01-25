package com.continuuity.passport.dal.db;

import com.continuuity.passport.core.User;
import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.dal.UserDAO;

import java.util.Map;


/**
 *  Implementation of UserDataAccess object using database as the persistence store
 */

public class UserDBAccess implements UserDAO {

  private final Map<String,String> configuration;

  public UserDBAccess(final Map<String, String> configuration){
    this.configuration = configuration;
  }

  /**
   * Create User in the System
   *
   * @param id   User id
   * @param user Instance of {@code User}
   * @return boolean status of creating user
   * @throws {@code RetryException}
   */
  @Override
  public boolean createUser(String id, User user) throws RetryException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Update User params
   *
   * @param id   userId
   * @param user Instance of {@code User}
   * @return boolean status of update user
   * @throws {@code RetryException}
   */
  @Override
  public boolean updateUser(String id, User user) throws RetryException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Delete User
   *
   * @param id userId
   * @return  status of delete user
   * @throws {@code RetryException}
   */
  @Override
  public boolean deleteUser(String id) throws RetryException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get User
   *
   * @param userId String
   * @return Instance of {@code User}
   * @throws {@code RetryException}
   */
  @Override
  public User getUser(String userId) throws RetryException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
