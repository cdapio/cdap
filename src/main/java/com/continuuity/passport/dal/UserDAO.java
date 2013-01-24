package com.continuuity.passport.dal;

import com.continuuity.passport.core.User;

/**
 *  Data Access interface for User operations
 */
public interface UserDAO {

  /**
   * Create User in the System
   * @param id User id
   * @param user Instance of {@code User}
   * @return boolean
   * @throws RuntimeException
   */
  public  boolean createUser(String id, User user) throws RuntimeException;

  /**
   * Update User params
   * @param id userId
   * @param user Instance of {@code User}
   * @return
   * @throws RuntimeException
   */
  public  boolean updateUser(String id, User user) throws RuntimeException;

  /**
   * Delete User
   * @param id userId
   * @return
   * @throws RuntimeException
   */
  public  boolean deleteUser(String id) throws RuntimeException;

  /**
   * Get User
   * @param userId String
   * @return Instance of {@code User}
   * @throws RuntimeException
   */
  public User getUser(String userId) throws RuntimeException;

}
