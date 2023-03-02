/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.app.store;

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.proto.id.ApplicationId;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;

/**
 * This class defined various filters that can be applied during applicaiton store scanning. The
 * filters itself are pushed down, so only the ones defined in this class and it's inner classes are
 * usually supported. Attempt to use any other will result in an {@link
 * UnsupportedOperationException}. Different filter types analyze different attributes and operate
 * on those attributes without full information retrieval / deserialization.
 */
public abstract class ApplicationFilter {

  /**
   * Base class for filters that filter on {@link ApplicationId} data. Those are the fastest filter
   * as they don't need full application record.
   */
  public abstract static class ApplicationIdFilter extends ApplicationFilter implements
      Predicate<ApplicationId> {

  }

  /**
   * Base class for filters that filter on application {@link ArtifactId}.
   */
  public abstract static class ArtifactIdFilter extends ApplicationFilter implements
      Predicate<ArtifactId> {

  }

  /**
   * The filter that check if application id contains a substring (ignoring case).
   */
  public static class ApplicationIdContainsFilter extends ApplicationIdFilter {

    private final String searchFor;

    public ApplicationIdContainsFilter(String searchFor) {
      this.searchFor = searchFor.toLowerCase(Locale.ROOT);
    }

    @Override
    public boolean test(ApplicationId applicationId) {
      return applicationId.getApplication().toLowerCase(Locale.ROOT).contains(searchFor);
    }

    @Override
    public String toString() {
      return "ApplicationIdContainsFilter{"
          + "searchFor='" + searchFor + '\''
          + '}';
    }
  }

  /**
   * The filter that check if application id exactly equals to a string (ignoring case).
   */
  public static class ApplicationIdEqualsFilter extends ApplicationIdFilter {

    private final String searchFor;

    public ApplicationIdEqualsFilter(String searchFor) {
      this.searchFor = searchFor;
    }

    @Override
    public boolean test(ApplicationId applicationId) {
      return applicationId.getApplication().equalsIgnoreCase(searchFor);
    }

    @Override
    public String toString() {
      return "ApplicationIdEqualsFilter{"
          + "searchFor='" + searchFor + '\''
          + '}';
    }
  }

  /**
   * Returns true if the application artifact is in an allowed list of names
   */
  public static class ArtifactNamesInFilter extends ArtifactIdFilter {

    private final Set<String> names;

    public ArtifactNamesInFilter(Set<String> names) {
      this.names = names;
    }

    @Override
    public boolean test(ArtifactId input) {
      return names.contains(input.getName());
    }

    @Override
    public String toString() {
      return "ArtifactNamesInFilter{"
          + "names=" + names
          + '}';
    }
  }

  /**
   * Returns true if the application artifact is a specific version
   */
  public static class ArtifactVersionFilter extends ArtifactIdFilter {

    private final String version;

    public ArtifactVersionFilter(String version) {
      this.version = version;
    }

    @Override
    public boolean test(ArtifactId input) {
      return version.equals(input.getVersion().getVersion());
    }

    @Override
    public String toString() {
      return "ArtifactVersionFilter{"
          + "version='" + version + '\''
          + '}';
    }
  }
}
