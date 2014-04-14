/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Contains collection of classes representing different types of Ids.
 */
public final class Id  {

  /**
   * Represents ID of an account.
   */
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

      return id.equals(((Account) o).id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id);
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

    public static Application from(Account id, String application) {
      return new Application(id, application);
    }

    public static Application from(String accountId, String applicationId) {
      return new Application(Id.Account.from(accountId), applicationId);
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

    public static Program from(Application appId, String pgmId) {
      return new Program(appId, pgmId);
    }

    public static Program from(String accountId, String appId, String pgmId) {
      return new Program(new Application(new Account(accountId), appId), pgmId);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("ProgramId(");

      sb.append("accountId:");
      if (this.application.getAccountId() == null) {
        sb.append("null");
      } else {
        sb.append(this.application.getAccountId());
      }
      sb.append(", applicationId:");
      if (this.application.getId() == null) {
        sb.append("null");
      } else {
        sb.append(this.application.getId());
      }
      sb.append(", runnableId:");
      if (this.id == null) {
        sb.append("null");
      } else {
        sb.append(this.id);
      }
      sb.append(")");
      return sb.toString();
    }
  }

}
