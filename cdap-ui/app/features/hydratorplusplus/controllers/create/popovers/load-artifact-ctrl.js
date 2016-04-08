/*
 * Copyright © 2016 Cask Data, Inc.
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

angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('LoadArtifactCtrl', function(myPipelineApi, $q, $stateParams, myAlertOnValium, $state, GLOBALS, myFileUploader) {
    this.jarStatus = -1; // 0 = loading, 1 = loadedSuccessfully , 2 = loadFailed
    this.jsonStatus = -1;
    this.myPipelineApi = myPipelineApi;
    this.$q = $q;
    this.$stateParams = $stateParams;
    this.myAlertOnValium = myAlertOnValium;
    this.$state = $state;
    this.myFileUploader = myFileUploader;
    this.artifactName = '';
    this.artifactVersion = '';
    this.GLOBALS = GLOBALS;
    this.jarLoadFailMessage = GLOBALS.en.hydrator.studio.info['ARTIFACT-UPLOAD-MESSAGE-JAR'];
    this.jsonLoadFailMessage = GLOBALS.en.hydrator.studio.info['ARTIFACT-UPLOAD-MESSAGE-JSON'];

    var jarFile, jsonFile, artifactExtends;
    this.openJARFileDialog = () => {
      document.getElementById('jar-import-config-link').click();
    };
    this.openJsonFileDialog = () => {
      document.getElementById('json-import-config-link').click();
    };
    let getArtifactNameAndVersion = (nameWithVersion) => {
      let regExpRule = new RegExp('(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?(?:[.\\-](.*))?$');
      let version = regExpRule.exec(nameWithVersion)[0];
      let name = nameWithVersion.substr(0, nameWithVersion.indexOf(version) -1);
      return { version, name };
    };
    this.loadJAR = (jar) => {
      if (jar[0].name.indexOf('.jar') === -1) {
        this.jarStatus = 2;
        return;
      }
      jarFile = jar[0];
      let nameAndVersion = jar[0].name.split('.jar')[0];
      var {name, version} = getArtifactNameAndVersion(nameAndVersion);
      this.artifactName = name;
      this.artifactVersion = version;
      this.jarStatus = 1;
    };
    let getJsonContents = (file) => {
      let defer = this.$q.defer();

      var reader = new FileReader();
      reader.readAsText(file, 'UTF-8');

      reader.onload = function (evt) {
        var result = evt.target.result;
        defer.resolve(result);
      };

      reader.onerror = function (evt) {
        defer.reject(evt);
      };
      return defer.promise;
    };

    this.loadJSON = (json) => {
      if (json[0].name.indexOf('.json') === -1) {
        this.jsonStatus = 2;
        return;
      }
      let artifactJson;
      getJsonContents(json[0]).then(
        (result) => {
          artifactJson = result;
          try {
            artifactJson = JSON.parse(artifactJson);
          } catch(e) {
            throw e;
          }
          if (!artifactJson.properties || !artifactJson.parents) {
            throw 'error';
          }
          jsonFile = artifactJson.properties;
          artifactExtends = artifactJson.parents.reduce( (prev, curr) => `${prev}/${curr}`);
          this.jsonStatus = 1;
        },
        () => {
          this.jsonStatus = 2;
          console.log('Got to LoadJar');
          this.jsonLoadFailMessage = this.GLOBALS.en.hydrator.studio.info['ARTIFACT-UPLOAD-ERROR-JSON'];
        }
      );
    };
    this.upload = () => {
      let params = {
        namespace: this.$stateParams.namespace,
        artifactName: this.artifactName
      };
      let jsonParams = angular.extend({
        version: this.artifactVersion
      }, params);

      this.myFileUploader.upload({
        path: '/namespaces/' + this.$stateParams.namespace + '/artifacts/' + this.artifactName,
        file: jarFile
      } , {
        'Content-type': 'application/octet-stream',
        'customHeader': {
          'Artifact-Version': this.artifactVersion,
          'Artifact-Extends': artifactExtends
        }
      })
        .then(
          () => this.myPipelineApi.loadJson(jsonParams, jsonFile).$promise,
          (err) => {
            this.errorMessage = 'Upload jar failed: ' + err;
            return $q.reject(this.errorMessage);
          }
        )
        .then(
          () => {
            this.$state.go('hydratorplusplus.create', this.$stateParams, { reload: true })
              .then(() => {
                this.myAlertOnValium.show({
                  type: 'success',
                  content: 'Artifact loaded successfully.'
                });
              });
          },
          (err) => {
            console.log('adasdasd ', err);
          }
        );
    };
  });
