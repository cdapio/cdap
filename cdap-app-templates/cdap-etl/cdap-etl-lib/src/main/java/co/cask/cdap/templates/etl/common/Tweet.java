/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.common;

import com.google.common.base.Objects;
import twitter4j.GeoLocation;

import java.util.Date;
import javax.annotation.Nullable;

/**
 * POJO that represents a Tweet.
 */
public class Tweet {

  private final long id;
  private final String message;
  private final String language;
  private final Date creationTime;
  private final int favoriteCount;
  private final int retweetCount;
  private final String source;
  private final GeoLocation geoLocation;
  private final boolean isRetweet;

  public Tweet(long id, String message, String language, Date creationTime, int favoriteCount, int retweetCount,
               String source, GeoLocation geoLocation, boolean retweet) {
    this.id = id;
    this.message = message;
    this.language = language;
    this.creationTime = creationTime;
    this.favoriteCount = favoriteCount;
    this.retweetCount = retweetCount;
    this.source = source;
    this.geoLocation = geoLocation;
    isRetweet = retweet;
  }

  public long getId() {
    return id;
  }

  public String getMessage() {
    return message;
  }

  public String getLanguage() {
    return language;
  }

  public Date getCreationTime() {
    return creationTime;
  }

  public int getFavoriteCount() {
    return favoriteCount;
  }

  public int getRetweetCount() {
    return retweetCount;
  }

  public String getSource() {
    return source;
  }

  @Nullable
  public GeoLocation getGeoLocation() {
    return geoLocation;
  }

  public boolean isRetweet() {
    return isRetweet;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", id)
      .add("message", message)
      .add("language", language)
      .add("creationTime", creationTime)
      .add("favoriteCount", favoriteCount)
      .add("retweetCount", retweetCount)
      .add("source", source)
      .add("geoLocation", geoLocation)
      .add("isRetweet", isRetweet)
      .toString();
  }
}
