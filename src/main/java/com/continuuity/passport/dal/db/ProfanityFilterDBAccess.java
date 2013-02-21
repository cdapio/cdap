package com.continuuity.passport.dal.db;

import com.continuuity.passport.dal.ProfanityFilter;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 *
 */
public class ProfanityFilterDBAccess extends DBAccess  implements ProfanityFilter{


  private Set<String> profanityDictionary = new HashSet<String>();

  private Set<String> whiteListWords = new HashSet<String>();

  private Pattern p = Pattern.compile("^[a-zA-Z0-9]+$");

  private String connectionString;

  public ProfanityFilterDBAccess(String connectionString) {
    this.connectionString = connectionString;
  }

  private void loadProfaneDictionary() {
    Connection connection =null;
    PreparedStatement ps = null;
    ResultSet rs  = null;
    try {
      connection = DriverManager.getConnection(connectionString);
      String SQL = String.format("SELECT %s FROM %s",
        DBUtils.Profanity.PROFANE_WORDS,
        DBUtils.Profanity.TABLE_NAME);

      ps = connection.prepareStatement(SQL);
      rs = ps.executeQuery();

      while (rs.next()) {
        String profaneName = rs.getString(1);
        this.profanityDictionary.add(profaneName);
      }

    } catch (SQLException e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    } finally {
      close(connection, ps);
    }
  }



  @Override
  /**
   * Filter words based on a set of criteria.
   * Criteria for filtering out.
   * Word is not filtered out if it passes all  of the condition below. Even if it fails one criteria the word is
   * filtered out.
   *   0) Passed in word is not in the white-list
   *   1) Passed in word should contain only a-z, A-Z, 0-9  - i.e., no special characters
   *   2) Strip out the numbers and see if the word matches any word in the profane dictionary
   */
  public boolean isFiltered(String data) {

    //0. if the word is in the whitelist. Don't filter it out
   if (whiteListWords.contains(data)) {
     return false;
   }

    //TODO: Put this dictionary in a cache and load it periodically
   if ( profanityDictionary.isEmpty()) {
     loadProfaneDictionary();
   }

   //1. if the word has special characters other than a-z, A-Z, 0-9 - Filter out
   if ( ! p.matcher(data).matches()) {
     return true;
   }

   String numberStripped = data.replaceAll("[^a-zA-Z]","");
   //2. If the word without numbers is in profane dictionary filter it out.
   if (profanityDictionary.contains(numberStripped)) {
      return true;
   }

    return false;
  }

}
