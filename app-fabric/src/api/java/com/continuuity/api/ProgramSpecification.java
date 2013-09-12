/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api;

/**
 * This interface provides specifications of any type of program.
 */
public interface ProgramSpecification {

  /**
   * @return Class name of the program.
   */
  String getClassName();

  /**
   * @return Name of the program.
   */
  String getName();

  /**
   * @return Description of the program.
   */
  String getDescription();
}
