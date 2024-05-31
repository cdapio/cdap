/*
 * Copyright © 2016 2016 Cask Data, Inc.
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
/**
 * Classes in this package implement row salting approach to distribute writes with monotonically
 * increasing keys to multiple regions.
 * TODO: the code was mostly copied from open-source project, we need to clean up stuff quite a bit.
 */
package io.cdap.cdap.hbase.wd;
