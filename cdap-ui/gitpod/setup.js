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

const branchInfo = require('./bamboo_plan_info.json');
const path = require('path');
var xml2js = require('xml2js');
const fs = require('fs');
const fetch = require('node-fetch');
const unzip = require('./unzip');
const start = require('./start');


// Get CDAP version from the pom file.
async function getCDAPVersion() {
  return new Promise((resolve, reject) => {
    var parser = new xml2js.Parser();
    fs.readFile(path.join(__dirname , '..', '..', 'pom.xml'), function(err, data) {
      if (err) {
        reject(err);
      }
      parser.parseString(data, function (err, result) {
        if (err) {
          reject(err);
        } 
        const version = result.project.version[0];
        resolve(version);
      });
    });
  });
}

async function downloadSDK(res, pathToZipFile) {
  const fileStream = fs.createWriteStream(pathToZipFile);
  return new Promise((resolve, reject) => {
    res.body.pipe(fileStream);
    res.body.on("error", reject);
    fileStream.on("finish", () => {
      console.log('Done downloading!');
      resolve();
    });
  });
}

async function main() {
  // Get the last successful build from appropriate plan branch. This plan branch corresponds to bamboo plan name for respective branches
  const branchResultsRaw = await fetch(`https://builds.cask.co/rest/api/latest/result/${branchInfo.planName}.json?buildstate=successful&max-result=1`);
  const branchResults = await branchResultsRaw.json();
  if (!branchResults.results.result.length) {
    throw `No successful build found in ${branchInfo.planName}`;
  }
  const latestSuccessfulBuildName = branchResults.results.result[0].key;
  let cdapversion;
  try {
    cdapversion = await getCDAPVersion();
    console.log('CDAP version: ', cdapversion);
  } catch(e) {
    throw 'Unable to fetch CDAP version from pom.xml file';
  }

  // Based on the version of CDAP construct the path to the SDK zip file from bamboo build plan.
  const SDKZipPath = `https://builds.cask.co/browse/${latestSuccessfulBuildName}/artifact/shared/SDK/cdap/cdap-standalone/target/cdap-sandbox-${cdapversion}.zip`;
  console.log('SDK zip path: ', SDKZipPath);
  let res;
  try {
    res = await fetch(SDKZipPath);
  } catch(e) {
    throw `Invalid url to SDK zip file: ${SDKZipPath} - ${e}` 
  }

  // Download and unzip SDK
  const pathToZipFile = path.join('/workspace', `cdap-sandbox-${cdapversion}.zip`);
  try {
    await downloadSDK(res, pathToZipFile);
    await unzip(pathToZipFile);
    console.log(`SDK is unzipped and ready to start!`);
  } catch(e) {
    console.error('Unable to download or unzip the SDK');
    throw e.message;
  }

  try {
    await start(path.join('/workspace', `cdap-sandbox-${cdapversion}`, 'bin', 'cdap'));
  } catch(e) {
    throw e;
  }
}

main();
