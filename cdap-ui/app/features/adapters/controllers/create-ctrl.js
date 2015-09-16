/*
 * Copyright © 2015 Cask Data, Inc.
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


angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterCreateController', function($timeout, $state, $alert) {
    this.importFile = function(files) {
      var reader = new FileReader();
      reader.readAsText(files[0], 'UTF-8');

      reader.onload = function (evt) {
         var data = evt.target.result;
         var jsonData;
         try {
           jsonData = JSON.parse(data);
         } catch(e) {
           $alert({
             type: 'danger',
             content: 'Error in the JSON imported.'
           });
           console.log('ERROR in imported json: ', e);
           return;
         }
         $state.go('adapters.create.studio', {
           data: jsonData,
           type: jsonData.artifact.name
         });
      };
    };

    this.openFileBrowser = function() {
      $timeout(function() {
        document.getElementById('adapter-import-config-link').click();
      });
    };

  });
