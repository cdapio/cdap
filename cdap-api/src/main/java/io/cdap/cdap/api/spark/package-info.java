/*
 * Copyright © 2014 Cask Data, Inc.
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
 * Defines APIs for defining Spark jobs in CDAP. Also provides a custom JavaSparkContext (for Java
 * Spark jobs) and SparkContext (for Scala Spark jobs) which supports reading a {@link
 * io.cdap.cdap.api.dataset.Dataset} to RDD and writing a RDD to {@link
 * io.cdap.cdap.api.dataset.Dataset}
 */
package io.cdap.cdap.api.spark;
