/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.base.CharMatcher;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.common.InvalidMetadataException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.metadata.MetadataConstants;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Utility to validate metadata keys and values.
 */
public class MetadataValidator {

  private static final CharMatcher KEY_AND_TAG_MATCHER = CharMatcher.inRange('A', 'Z')
    .or(CharMatcher.inRange('a', 'z'))
    .or(CharMatcher.inRange('0', '9'))
    .or(CharMatcher.is('_'))
    .or(CharMatcher.is('-'));

  private static final CharMatcher VALUE_MATCHER = CharMatcher.inRange('A', 'Z')
    .or(CharMatcher.inRange('a', 'z'))
    .or(CharMatcher.inRange('0', '9'))
    .or(CharMatcher.is('_'))
    .or(CharMatcher.is('-'))
    .or(CharMatcher.WHITESPACE);

  private final int maxCharacters;

  /**
   * Constructor only takes the configuration to determine the maximal allowed length for a key.
   */
  public MetadataValidator(CConfiguration cConf) {
    maxCharacters = cConf.getInt(Constants.Metadata.MAX_CHARS_ALLOWED);
  }

  /**
   * Validate metadata properties.
   *
   * @param metadataEntity the target entity
   * @param properties the properties to be set
   * @throws InvalidMetadataException if any of the keys or values are invalid
   */
  public void validateProperties(MetadataEntity metadataEntity,
                                 @Nullable Map<String, String> properties) throws InvalidMetadataException {

    if (null == properties || properties.isEmpty()) {
      return;
    }
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      // validate key
      validateKeyAndTagsFormat(metadataEntity, entry.getKey());
      validateTagReservedKey(metadataEntity, entry.getKey());
      validateLength(metadataEntity, entry.getKey());

      // validate value
      validateValueFormat(metadataEntity, entry.getValue());
      validateLength(metadataEntity, entry.getValue());
    }
  }

  /**
   * Validate metadata tags.
   *
   * @param metadataEntity the target entity
   * @param tags the tags to be set
   * @throws InvalidMetadataException if any of the keys or values are invalid
   */
  public void validateTags(MetadataEntity metadataEntity,
                           @Nullable Set<String> tags) throws InvalidMetadataException {
    if (null == tags || tags.isEmpty()) {
      return;
    }
    for (String tag : tags) {
      validateKeyAndTagsFormat(metadataEntity, tag);
      validateLength(metadataEntity, tag);
    }
  }

  /**
   * Validate that the key is not reserved {@link MetadataConstants#TAGS_KEY}.
   */
  private static void validateTagReservedKey(MetadataEntity metadataEntity, String key)
    throws InvalidMetadataException {
    if (MetadataConstants.TAGS_KEY.equals(key.toLowerCase())) {
      throw new InvalidMetadataException(
        metadataEntity, "Could not set metadata with reserved key " + MetadataConstants.TAGS_KEY);
    }
  }

  /**
   * Validate the key matches the {@link #KEY_AND_TAG_MATCHER} character test.
   */
  private static void validateKeyAndTagsFormat(MetadataEntity metadataEntity, String keyword)
    throws InvalidMetadataException {
    if (!KEY_AND_TAG_MATCHER.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(metadataEntity, String.format(
        "Illegal format for '%s'. Should only contain alphanumeric characters (a-z, A-Z, 0-9), " +
          "underscores and hyphens.", keyword));
    }
  }

  /**
   * Validate the value of a property matches the {@link #VALUE_MATCHER} character test.
   */
  private static void validateValueFormat(MetadataEntity metadataEntity, String keyword)
    throws InvalidMetadataException {
    if (!VALUE_MATCHER.matchesAllOf(keyword)) {
      throw new InvalidMetadataException(metadataEntity, String.format(
        "Illegal format for the value '%s'. Should only contain alphanumeric characters (a-z, A-Z, 0-9), " +
          "underscores, hyphens and whitespaces.", keyword));
    }
  }

  /**
   * Validate that the key length does not exceed the configured limit.
   */
  private void validateLength(MetadataEntity metadataEntity, String keyword) throws InvalidMetadataException {
    // check for max char per value
    if (keyword.length() > maxCharacters) {
      throw new InvalidMetadataException(
        metadataEntity, "Metadata " + keyword + " exceeds maximum of " + maxCharacters + " characters.");
    }
  }

}
