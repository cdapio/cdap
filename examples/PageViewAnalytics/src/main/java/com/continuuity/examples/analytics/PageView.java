/**
 * Copyright 2013-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.examples.analytics;

/**
 *  A PageView tracks users viewing from the referrer pages to the requested pages.
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
