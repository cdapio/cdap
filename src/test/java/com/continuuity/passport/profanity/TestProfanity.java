package com.continuuity.passport.profanity;

import com.continuuity.passport.dal.db.ProfanityFilterFileAccess;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestProfanity {


  @Test
  public void testProfanity () throws SQLException, ClassNotFoundException {

    //TODO: Place holder for DBProfanity
//    TestHelper.startHsqlDB();
//
//    Map<String,String> config = new HashMap<String,String>();
//    config.put("connectionString","jdbc:hsqldb:mem:test?user=sa");
//    config.put("jdbcType", "mysql" );


    String profanePath =  getClass().getResource("/ProfaneWords").getPath();

    //ProfanityFilterDBAccess profanityFilter = new ProfanityFilterDBAccess("jdbc:hsqldb:mem:test?user=sa");
    ProfanityFilterFileAccess profanityFilter = new ProfanityFilterFileAccess(profanePath);

    assertFalse(profanityFilter.isFiltered("data"));
    assertTrue(profanityFilter.isFiltered("d23@#@"));
    assertTrue(profanityFilter.isFiltered("fuck"));
    assertTrue(profanityFilter.isFiltered("fuck123"));
    assertTrue(profanityFilter.isFiltered("212fuck"));
    assertTrue(profanityFilter.isFiltered("f1u1c1k"));


 //TODO: Placeholder for future
 //   TestHelper.stopHsqlDB();



  }

}
