/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 *
 */
public final class Id  {

  private static String DEFAULT_ACCOUNT_ID = "demo";
  private static String DEFAULT_APPLICATION_ID = "myapp";
  private static String DEFAULT_PROGRAM_ID = "pgm";

  public static final class Account {
    private final String id;

    public Account(String id) {
      Preconditions.checkNotNull(id, "Account cannot be null.");
      this.id = id;
    }

    public String getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      return id.equals(((Account)o).id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id);
    }

    public static Account DEFAULT() {
      return new Account(DEFAULT_ACCOUNT_ID);
    }

    public static Account from(String account) {
      return new Account(account);
    }
  }

  /**
   * Program Id identifies a given application.
   * Application is global unique if used within context of account.
   */
  public static final class Application {
    private final Account account;
    private final String applicationId;

    public Application(final Account account, final String applicationId) {
      Preconditions.checkNotNull(account, "Account cannot be null.");
      Preconditions.checkNotNull(applicationId, "Application cannot be null.");
      this.account = account;
      this.applicationId = applicationId;
    }

    public Account getAccount() {
      return account;
    }

    public String getAccountId() {
      return account.getId();
    }

    public String getId() {
      return applicationId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Application that = (Application) o;
      return account.equals(that.account) && applicationId.equals(that.applicationId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(account, applicationId);
    }

    public static Application DEFAULT() {
      return new Application(Account.DEFAULT(), DEFAULT_APPLICATION_ID);
    }

    public static Application from(Account id, String application) {
      return new Application(id, application);
    }
  }

  /**
   * Program Id identifies a given program.
   * Program is global unique if used within context of account and application.
   */
  public static class Program {
    private final Application application;
    private final String id;

    public Program(Application application, final String id) {
      Preconditions.checkNotNull(application, "Application cannot be null.");
      Preconditions.checkNotNull(id, "Id cannot be null.");
      this.application = application;
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public String getApplicationId() {
      return application.getId();
    }

    public String getAccountId() {
      return application.getAccountId();
    }

    public Application getApplication() {
      return application;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Program program = (Program) o;
      return application.equals(program.application) && id.equals(program.id);
    }

    @Override
    public int hashCode() {
      int result = application.hashCode();
      result = 31 * result + id.hashCode();
      return result;
    }

    public static Program from(Account id) {
      return new Program(new Application(id, DEFAULT_APPLICATION_ID), DEFAULT_PROGRAM_ID);
    }

    public static Program from(Account accountId, String appId) {
      return new Program(new Application(accountId, appId), DEFAULT_PROGRAM_ID);
    }

    public static Program from(Application appId) {
      return new Program(appId, DEFAULT_PROGRAM_ID);
    }

    public static Program from(Application appId, String pgmId) {
      return new Program(appId, pgmId);
    }

    public static Program from(String accountId, String appId, String pgmId) {
      return new Program(new Application(new Account(accountId), appId), pgmId);
    }

    public static Program DEFAULT() {
      return new Program(Application.DEFAULT(), DEFAULT_PROGRAM_ID);
    }
  }

}
