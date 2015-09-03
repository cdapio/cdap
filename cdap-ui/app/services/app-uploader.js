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

angular.module(PKG.name + '.services')
  .factory('myAppUploader', function(myFileUploader, $state, $alert, myAlert) {
    function upload(files, namespace) {

      for (var i = 0; i < files.length; i++) {
        myFileUploader.upload({
          path: '/namespaces/' + ($state.params.namespace || namespace) + '/apps',
          file: files[i]

        }, 'application/octet-stream')
          .then(success,error);
      }

      function success() {
        $alert({
          type: 'success',
          title: 'Upload success',
          content: 'The application has been uploaded successfully'
        });
        $state.reload();
      }

      // Independent xhr request. Failure case will not be handled by $rootScope.
      function error(err) {
        myAlert({
          type: 'danger',
          title: 'Upload failed',
          content: err || ''
        });
      }
    }

    return {
      upload: upload
    };
  });
