/*
 * Copyright © 2026 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.common.InvalidMetadataException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link MetadataValidator}.
 */
public class MetadataValidatorTest {

  private static final MetadataEntity ENTITY = MetadataEntity.ofDataset("ns1", "ds1");

  @Test
  public void testValidPropertiesWithWhitespaceAndTags() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    MetadataValidator validator = new MetadataValidator(cConf);

    validator.validateProperties(ENTITY, null);
    validator.validateProperties(ENTITY, Collections.emptyMap());
    validator.validateTags(ENTITY, null);
    validator.validateTags(ENTITY, Collections.emptySet());

    // VALUE_MATCHER allows ASCII and Unicode whitespace alongside alphanumerics, '_', and '-'.
    validator.validateProperties(
        ENTITY,
        ImmutableMap.of(
            "key_1-A", "value with spaces\tand\nnewlines\u2003ok",
            "empty_val", ""));

    validator.validateTags(ENTITY, ImmutableSet.of("tag_1", "Tag-2", "123"));
  }

  @Test
  public void testRejectsWhitespaceInKeysAndTagsAndInvalidValueChars() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Metadata.MAX_CHARS_ALLOWED, 10);
    MetadataValidator validator = new MetadataValidator(cConf);

    // Key with whitespace is rejected.
    try {
      validator.validateProperties(ENTITY, ImmutableMap.of("bad key", "ok"));
      Assert.fail("Expected InvalidMetadataException for whitespace in key");
    } catch (InvalidMetadataException expected) {
      // Expected.
    }

    // Tag with whitespace is rejected.
    try {
      validator.validateTags(ENTITY, ImmutableSet.of("bad\ttag"));
      Assert.fail("Expected InvalidMetadataException for whitespace in tag");
    } catch (InvalidMetadataException expected) {
      // Expected.
    }

    // Value with punctuation outside '_', '-', and whitespace is rejected.
    try {
      validator.validateProperties(ENTITY, ImmutableMap.of("valid_key", "bad@value"));
      Assert.fail("Expected InvalidMetadataException for invalid character in value");
    } catch (InvalidMetadataException expected) {
      // Expected.
    }

    // Non-breaking space \u00A0 IS matched by Guava CharMatcher.WHITESPACE (allowed in values,
    // rejected in keys). Zero-width space \u200B is NOT in CharMatcher.WHITESPACE (rejected).
    validator.validateProperties(ENTITY, ImmutableMap.of("valid_key", "nbsp\u00A0val"));
    try {
      validator.validateProperties(ENTITY, ImmutableMap.of("nbsp\u00A0key", "ok"));
      Assert.fail("Expected InvalidMetadataException for non-breaking space in key");
    } catch (InvalidMetadataException expected) {
      // Expected.
    }
    try {
      validator.validateProperties(ENTITY, ImmutableMap.of("valid_key", "zwsp\u200Bval"));
      Assert.fail("Expected InvalidMetadataException for zero-width space in value");
    } catch (InvalidMetadataException expected) {
      // Expected.
    }

    // Reserved "tags" key (case-insensitive) is rejected.
    try {
      validator.validateProperties(ENTITY, ImmutableMap.of("TAGS", "val"));
      Assert.fail("Expected InvalidMetadataException for reserved tags key");
    } catch (InvalidMetadataException expected) {
      // Expected.
    }

    // Exceeding max length is rejected.
    try {
      validator.validateProperties(ENTITY, ImmutableMap.of("k", "01234567890"));
      Assert.fail("Expected InvalidMetadataException for exceeding max length");
    } catch (InvalidMetadataException expected) {
      // Expected.
    }
  }
}
