/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi;

/**
 * Version information that is comparable to other instance of the same class or any other Object that
 * returns properly formatted version from it's {@link Object#toString()}. E.g. you can compare it to a string.
 * Also any instance of this interface must return proper bare version from it's toString.
 */
public interface VersionInfo extends Comparable<Object> {

}
