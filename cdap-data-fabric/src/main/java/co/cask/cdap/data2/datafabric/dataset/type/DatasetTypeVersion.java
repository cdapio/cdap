/*
 * *
 *  Copyright © 2014 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 * /
 */

package co.cask.cdap.data2.datafabric.dataset.type;

/**
 * Dataset Type Version and checksum information
 */
public class DatasetTypeVersion {
  String moduleName;
  int version;
  String checksum; //this should be byte array or something else

  public DatasetTypeVersion(String moduleName, int version, String checksum) {
    this.moduleName = moduleName;
    this.version = version;
    this.checksum = checksum;
  }

  public String getModuleName() {
    return moduleName;
  }

  public int getVersion() {
    return version;
  }

  public String getChecksum() {
    return checksum;
  }

}
