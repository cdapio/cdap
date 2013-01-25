package com.continuuity.passport.dal;

import com.continuuity.passport.core.User;
import com.continuuity.passport.core.exceptions.RetryException;

/**
 * Data Access interface for User operations
 */
public interface UserDAO {

  /**
   * Create User in the System
   *
   * @param id   User id
   * @param user Instance of {@code User}
   * @return boolean
   * @throws {@code RetryException}
   */
  public boolean createUser(String id, User user) throws RetryException;

  /**
   * Update User params
   *
   * @param id   userId
   * @param user Instance of {@code User}
   * @return
   * @throws {@code RetryException}
   */
  public boolean updateUser(String id, User user) throws RetryException;

  /**
   * Delete User
   *
   * @param id userId
   * @return
   * @throws {@code RetryException}
   */
  public boolean deleteUser(String id) throws RetryException;

  /**
   * Get User
   *
   * @param userId String
   * @return Instance of {@code User}
   * @throws {@code RetryException}
   */
  public User getUser(String userId) throws RetryException;

}
