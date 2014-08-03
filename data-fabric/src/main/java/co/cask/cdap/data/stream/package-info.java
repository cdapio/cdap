/*
 * Copyright 2014 Cask, Inc.
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
 * This package contains classes for management and manipulation of stream files.
 *
 * Configurations:
 *
 * <pre>
 * {@code
 *   stream.base.dir - The directory root for all stream files, relative to the HDFS namespace.
 *   stream.default.partition.duration - The default duration of a stream partition in milliseconds.
 *   stream.default.index.interval - Default time interval in milliseconds for emitting new index entry in stream file.
 * }
 * </pre>
 */
package co.cask.cdap.data.stream;
