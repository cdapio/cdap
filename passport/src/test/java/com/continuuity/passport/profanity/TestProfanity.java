package com.continuuity.passport.profanity;

import com.continuuity.passport.dal.db.ProfanityFilterFileAccess;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Profanity filter tests.
 */
public class TestProfanity {

  @Test
  public void testProfanity () throws SQLException, ClassNotFoundException {

    String profanePath =  getClass().getResource("/ProfaneWords").getPath();

    ProfanityFilterFileAccess profanityFilter = new ProfanityFilterFileAccess(profanePath);

    //Host names that should be filtered. Does'nt match regex or has black listed words.
    assertTrue(profanityFilter.isFiltered("d23@#@"));
    assertTrue(profanityFilter.isFiltered("fuck"));
    assertTrue(profanityFilter.isFiltered("fuck123"));
    assertTrue(profanityFilter.isFiltered("212fuck"));
    assertTrue(profanityFilter.isFiltered("f1u1c1k"));
    assertTrue(profanityFilter.isFiltered("-no-start-with-hyphen"));
    assertTrue(profanityFilter.isFiltered("-9"));
    assertTrue(profanityFilter.isFiltered("------"));
    assertTrue(profanityFilter.isFiltered("no-last-char-hyphen-"));

    //Hostnames should should not be filtered.
    assertFalse(profanityFilter.isFiltered("data"));
    assertFalse(profanityFilter.isFiltered("this-is-valid"));


  }
}
