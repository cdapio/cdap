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
  .factory('myFileUploader', function($q, $window, $alert, cfpLoadingBar, myAuth, myAlert) {
    function upload(fileObj, contentType){
      var deferred = $q.defer();
      if (!myAuth.currentUser) {
        deferred.reject(400);
        myAlert({
          title: 'Must specify user: ',
          content: 'Could not find user.',
          type: 'danger'
        });
      } else {
        var xhr = new $window.XMLHttpRequest();
        xhr.upload.addEventListener('progress', function (e) {
          if (e.type === 'progress') {
            console.info('App Upload in progress');
          }
        });
        var path = fileObj.path;
        xhr.open('POST', path, true);
        xhr.setRequestHeader('Content-type', contentType);
        xhr.setRequestHeader('X-Archive-Name', fileObj.file.name);
        xhr.setRequestHeader('Authorization', 'Bearer ' + myAuth.currentUser.token);
        xhr.send(fileObj.file);
        cfpLoadingBar.start();
        xhr.onreadystatechange = function () {
          if (xhr.readyState === 4) {
            if (xhr.status > 399){
              deferred.reject(xhr.response);
            } else {
              deferred.resolve();
            }
            cfpLoadingBar.complete();
          }
        };
      }
      return deferred.promise;
    }


    return {
      upload: upload
    };
  });
