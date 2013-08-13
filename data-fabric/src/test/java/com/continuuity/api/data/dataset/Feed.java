package com.continuuity.api.data.dataset;

import java.util.List;

/**
 *
 */
public class Feed {

  private final String id;
  private final String url;
  private final List<String> categoriesList;

  public Feed(String id, String url, List<String> categoriesList) {
    this.id = id;
    this.url = url;
    this.categoriesList = categoriesList;
  }

  public String getId() {
    return id;
  }

  public String getUrl() {
    return url;
  }

  public List<String> getCategoriesList() {
    return categoriesList;
  }
}

