package com.continuuity.examples.logger3;

/**
 *  A PageView tracks users viewing from the referrer pages to requested pages.
 */
public class PageView {
  private final String referrer;
  private final String uri;

  public PageView(String referrer, String uri) {
    this.referrer = referrer;
    this.uri = uri;
  }

  public String getReferrer() {
    return referrer;
  }

  public String getUri() {
    return uri;
  }
}
