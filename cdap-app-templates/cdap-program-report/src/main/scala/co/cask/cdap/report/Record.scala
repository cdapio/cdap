/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.report

/**
  * Represents the full content of a report record.
  * TODO: [CDAP-13294] add artifact and app information
  */
case class Record(namespace: String, program: String, run: String, start: Option[Long], running: Option[Long],
                  end: Option[Long], duration: Option[Long], user: Option[String],
                  // use scala.collection.Map[String,String]] instead of Map[String, String] to avoid compilation error
                  // in Janino generated code
                  runtimeArgs: Option[scala.collection.Map[String, String]])
