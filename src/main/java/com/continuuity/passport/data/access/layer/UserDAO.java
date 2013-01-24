package com.continuuity.passport.data.access.layer;

import com.continuuity.passport.core.User;

/**
 *  Data Access interface for User operations
 */
public interface UserDAO {

  public  boolean createUser(String id, User user) throws RuntimeException;

  public  boolean updateUser(String id, User user) throws RuntimeException;

  public  boolean deleteUser(String id, User user) throws RuntimeException;

}
