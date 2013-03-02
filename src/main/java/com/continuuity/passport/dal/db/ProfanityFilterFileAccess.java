package com.continuuity.passport.dal.db;

import com.continuuity.passport.Constants;
import com.continuuity.passport.dal.ProfanityFilter;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
    try {
     Iterables.addAll(profanityDictionary, Files.readLines(new File(profaneFilePath), Charsets.UTF_8));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    LOG.info(String.format("Profanity Dictionary loaded %d words from %s",count, profaneFilePath));
  }
  @Inject
  public ProfanityFilterFileAccess(@Named(Constants.CFG_PROFANE_WORDS_FILE_PATH)String profaneFilePath) {
    this.profaneFilePath = profaneFilePath;
    loadProfaneDictionary();
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
