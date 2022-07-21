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

package io.cdap.cdap.k8s.util;

import org.junit.Test;

/**
 * Tests for {@link KubeUtil}.
 */
public class KubeUtilTest {
  @Test
  public void testValidateValidPathSegmentName() {
    KubeUtil.validatePathSegmentName("some-valid-name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateIllegalPathSegmentName() {
    KubeUtil.validatePathSegmentName(".");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidatePathSegmentNameWithIllegalCharacter() {
    KubeUtil.validatePathSegmentName("some-invalid-%-name");
  }

  @Test
  public void testValidateValidRFC1123LabelName() {
    KubeUtil.validateRFC1123LabelName("some-valid-name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateInvalidRFC1123LabelNameTooLong() {
    KubeUtil.validateRFC1123LabelName("some-invalid-name-too-long-some-invalid-name-too-long-some-invalid-name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateRFC1123LabelNameInvalidCharacter() {
    KubeUtil.validateRFC1123LabelName("some-invalid-&(^-name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateRFC1123LabelNameInvalidStartingCharacter() {
    KubeUtil.validateRFC1123LabelName("-some-invalid-name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateRFC1123LabelNameInvalidEndingCharacter() {
    KubeUtil.validateRFC1123LabelName("some-invalid-name-");
  }
}
