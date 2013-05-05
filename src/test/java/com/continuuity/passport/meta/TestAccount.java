package com.continuuity.passport.meta;

import org.junit.Test;

/**
 *
 */
public class TestAccount {

  @Test
  public void testAccount() {
    String json = "{\"first_name\":\"Sree\",\"last_name\":\"Raman\"," +
      "\"company\":\"Continuuity\",\"email_id\":\"born@to.run\",\"account_id\":1," +
      "\"api_key\":\"25b5243627dbf551110cf95884cfa23b11e6e3d8\",\"confirmed\":true}";

    Account account  = Account.fromString(json);

    assert ("Sree".equals(account.getFirstName()));
    assert ("Raman".equals(account.getLastName()));
    assert("Continuuity".equals(account.getCompany()));
    assert(1 == account.getAccountId());
    assert("25b5243627dbf551110cf95884cfa23b11e6e3d8".equals(account.getApiKey()));
    assert("born@to.run".equals(account.getEmailId()));

  }

}
