package com.continuuity.passport.profanity;

import com.continuuity.passport.dal.TestHelper;
import com.continuuity.passport.dal.db.ProfanityFilterDBAccess;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestProfanity {

  @Test
  public void testProfanity () throws SQLException, ClassNotFoundException {

    TestHelper.startHsqlDB();

    Map<String,String> config = new HashMap<String,String>();
    config.put("connectionString","jdbc:hsqldb:mem:test?user=sa");
    config.put("jdbcType", "mysql" );

    ProfanityFilterDBAccess profanityFilter = new ProfanityFilterDBAccess("jdbc:hsqldb:mem:test?user=sa");

    assertFalse(profanityFilter.isFiltered("data"));
    assertTrue(profanityFilter.isFiltered("d23@#@"));
    assertTrue(profanityFilter.isFiltered("fuck"));
    assertTrue(profanityFilter.isFiltered("fuck123"));
    assertTrue(profanityFilter.isFiltered("212fuck"));
    assertTrue(profanityFilter.isFiltered("f1u1c1k"));

    TestHelper.stopHsqlDB();



  }

}
