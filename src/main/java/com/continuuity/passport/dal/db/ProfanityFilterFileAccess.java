package com.continuuity.passport.dal.db;

import com.continuuity.passport.Constants;
import com.continuuity.passport.dal.ProfanityFilter;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 *
 */
public class ProfanityFilterFileAccess implements ProfanityFilter {

  private static final Logger LOG = LoggerFactory.getLogger(ProfanityFilterFileAccess.class);

  private Set<String> profanityDictionary = new HashSet<String>();

  private Pattern p = Pattern.compile("^[a-zA-Z0-9]+$");

  private String profaneFilePath;

  private void loadProfaneDictionary() {
    //Note: This is not an error case. This is the current way to disable this feature
    //TODO: Use DB to get profane words
    if ((profaneFilePath == null )|| (profaneFilePath.isEmpty())){
      LOG.info("Profanity dictionary not loaded");
      return;
    }
    int count = 0;
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(profaneFilePath));
      String line;
      while ((line = br.readLine()) != null) {
        profanityDictionary.add(line.toLowerCase());
        count++;
      }
    } catch (FileNotFoundException e) {
      throw Throwables.propagate(e);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    finally{
      try{
        if(br == null) {
          br.close();
        }
      }
      catch (IOException e){
        LOG.error("Exception while closing profanity file");
      }
    }
    LOG.info(String.format("Profanity Dictionary loaded %d words from %s",count, profaneFilePath));
  }
  @Inject
  public ProfanityFilterFileAccess(@Named(Constants.CFG_PROFANE_WORDS_FILE_PATH)String profaneFilePath) {
    this.profaneFilePath = profaneFilePath;
  }
  /**
   * Filter words based on a set of criteria.
   * Criteria for filtering out.
   * Word is not filtered out if it passes all  of the condition below. Even if it fails one criteria the word is
   * filtered out.
   *   0) Passed in word is not in the white-list
   *   1) Passed in word should contain only a-z, A-Z, 0-9  - i.e., no special characters
   *   2) Strip out the numbers and see if the word matches any word in the profane dictionary
   */
  @Override
  public boolean isFiltered(String data) {

    if ( profanityDictionary.isEmpty()) {
      loadProfaneDictionary();
    }

    //1. if the word has special characters other than a-z, A-Z, 0-9 - Filter out
    if ( ! p.matcher(data).matches()) {
      return true;
    }

    String numberStripped = data.replaceAll("[^a-zA-Z]","").toLowerCase();
    //2. If the word without numbers is in profane dictionary filter it out.
    if (profanityDictionary.contains(numberStripped)) {
      return true;
    }

    return false;
  }
}
