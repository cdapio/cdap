/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.spi.metadata;

import java.util.EnumSet;
import java.util.Set;

/**
 * Distinguishes different kinds of meta datum, e.g., tag or property.
 */
public enum MetadataKind {
  TAG,
  PROPERTY;

  public static final Set<MetadataKind> ALL = EnumSet.allOf(MetadataKind.class);
  public static final Set<MetadataKind> NONE = EnumSet.noneOf(MetadataKind.class);
}
