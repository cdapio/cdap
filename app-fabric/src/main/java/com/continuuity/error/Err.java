/*
 * Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.error;

/**
 * Centrally managed Err class where all the errors messages in the system are maintained.
 * <p>
 * Intent of centralizing is that they can be currated and managed in better way.
 * </p>
 */
public final class Err {
  /**
   * private constructor to prevent caller from creating Err object.
   */
  private Err() {

  }

  /**
   * Common Error messages that can be used in different contexts.
   */
  public static final Errors NOT_AN_ID = new Errors("%s name is not an ID. ID can contain only characters A-Za-z0-9_-");

  /**
   * Defines Schema related error messages.
   */
  public static class Schema {
    /**
     * Preventing construction.
     */
    private Schema() {
    }

    public static final Errors NOT_SUPPORTED_TYPE = new Errors(
      "Type %s is not supported. " +
        "Only Class or ParameterizedType are supported"
    );
  }

  /**
   * Defines Application level error messages.
   */
  public static class Application {
    /**
     * preventing construction
     */
    private Application() {
    }

    public static final Errors ATLEAST_ONE_PROCESSOR = new Errors(
      "Application %s has no Flow, Procedure, MapReduce job, or Service defined; " +
        "should have at least one of them defined"
    );
  }

  /**
   * Defines Flow specific error messages.
   */
  public static class Flow {
    /**
     * Preventing construction.
     */
    private Flow() {
    }

    public static final Errors ATLEAST_ONE_FLOWLET = new Errors(
      "Flow %s has no Flowlets defined; " +
        "should at least have a Flowlet"
    );
    public static final Errors ATLEAST_ONE_CONNECTION = new Errors(
      "Flow %s has no connections defined; " +
        "should at least have one connection"
    );

    public static final Errors OUTPUT_NOT_CONNECTED = new Errors(
      "In Flow '%s', Flowlet '%s' has outputs but no consumer"
    );

    public static final Errors NO_INPUT_FOR_OUTPUT = new Errors(
      "In Flow '%s', input of Flowlet '%s' does not define a compatible " +
        "type method for processing output of '%s' '%s'"
    );
    public static final Errors MORE_OUTPUT_NOT_ALLOWED = new Errors(
      "In Flow '%s', the %s '%s' has no consumer for outputs '%s'"
    );
    // Output being emitted
  }

  /**
   * Defines Procedure specific error messages.
   */
  public static class Procedure {
    /**
     * Preventing construction.
     */
    private Procedure() {
    }
  }

  /**
   * Defines Dataset specific error messages.
   */
  public static class DataSet {
    /**
     * Preventing Construction.
     */
    private DataSet() {
    }
  }

  /**
   * Defines Stream specific error messages.
   */
  public static class Stream {
    /**
     * Preventing Construction.
     */
    private Stream() {
    }
  }
}
