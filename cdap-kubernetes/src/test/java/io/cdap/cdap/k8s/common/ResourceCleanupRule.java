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

package io.cdap.cdap.k8s.common;

import java.util.ArrayList;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ExternalResource} that can be used in conjunction with {@link Rule} or {@link ClassRule} for
 * cleanup in unit-test.
 */
public class ResourceCleanupRule extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceCleanupRule.class);

  private final List<AutoCloseable> closeables = new ArrayList<>();

  /**
   * Registers a {@link AutoCloseable} resource such that the {@link AutoCloseable#close()} method will be called
   * at the end of the test.
   *
   * @param resource the {@link AutoCloseable} resource
   * @return the resource provided
   */
  public <T extends AutoCloseable> T register(T resource) {
    closeables.add(resource);
    return resource;
  }

  @Override
  protected void after() {
    for (AutoCloseable c : closeables) {
      try {
        c.close();
      } catch (Exception e) {
        LOG.warn("Exception raised when calling {}.close()", c.getClass().getName(), e);
      }
    }
  }
}
