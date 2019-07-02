/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/

/**
 * This snippet makes sure we setup the input data for pipelines to run before we kick off integration tests.
 *
 * This copies csv files from fixtures folder to /tmp/cdap-ui-integration-fixtures/ folder and all the pipelines
 * can refer the input from that specific path.
 */
var fs = require('fs');
var path = require('path');
const tmpPath = '/tmp/cdap-ui-integration-fixtures';

function copyFilesToTempDirectory() {
  let files;
  const fixturepath = path.join(__dirname, '/fixtures');
  try {
    files = fs.readdirSync(fixturepath);
  } catch (e) {
    console.log('Error: Unable to read cypress fixtures directory. ', e);
  }
  files.filter((file) => file.indexOf('.csv') !== -1).forEach((file) => {
    if (!fs.existsSync(tmpPath)) {
      fs.mkdirSync(tmpPath);
    }
    fs.copyFile(`${fixturepath}/${file}`, `${tmpPath}/${file}`, (err) => {
      if (err) {
        console.log(`Copying ${file} failed. `, err);
        return;
      }
    });
  });
}

function updateGCPEnvironmentVariables() {
  var serviceAccountFileContents = process.env.gcp_service_account_contents;
  if (!serviceAccountFileContents) {
    return;
  }
  if (serviceAccountFileContents) {
    if (!fs.existsSync(tmpPath)) {
      fs.mkdirSync(tmpPath);
    }
    try {
      fs.writeFileSync(`${tmpPath}/gcp_service_account.json`, serviceAccountFileContents, {});
    } catch (e) {
      console.log('Unable to write GCP service account file: ', e);
    }
  }
}

copyFilesToTempDirectory();
updateGCPEnvironmentVariables();
