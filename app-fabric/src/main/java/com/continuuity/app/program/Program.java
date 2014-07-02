/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.program;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import org.apache.twill.filesystem.Location;

/**
 * Abstraction of a executable program.
 */
public interface Program {

  /**
   * Returns the name of the main class in the program.
   */
  String getMainClassName();

  /**
   * Loads and returns the main class of the program
   * @throws ClassNotFoundException If fails to load the class.
   */
  <T> Class<T> getMainClass() throws ClassNotFoundException;

  /**
   * Returns the program type.
   */
  Type getType();

  /**
   * Returns the program ID.
   */
  Id.Program getId();

  /**
   * Returns name of the program.
   */
  String getName();

  /**
   * Returns the account ID that this program belongs to.
   */
  String getAccountId();

  /**
   * Returns the application ID that this program belongs to.
   */
  String getApplicationId();

  /**
   * Returns the complete application specification that contains this program.
   */
  ApplicationSpecification getSpecification();

  /**
   * Returns the location of the jar file of this program.
   */
  Location getJarLocation();

  /**
   * Returns the class loader for loading classes inside this program.
   */
  ClassLoader getClassLoader();
}
