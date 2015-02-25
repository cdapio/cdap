angular.module(PKG.name + '.services')
  .factory('myFileUploader', function($rootScope, $q, $window, $alert, cfpLoadingBar) {
    function upload(fileObj){
      var deferred = $q.defer();
      if (!$rootScope.currentUser) {
        deferred.reject(400);
        $alert({
          title: 'Must specify user: ',
          content: 'Could not find user.',
          type: 'danger'
        });
      } else {
        var xhr = new $window.XMLHttpRequest();
        xhr.upload.addEventListener('progress', function (e) {
          if (e.type === 'progress') {
            console.info('App Upload in progress!');
          }
        });
        var path = fileObj.path;
        xhr.open('POST', path, true);
        xhr.setRequestHeader('Content-type', 'application/octet-stream');
        xhr.setRequestHeader('X-Archive-Name', fileObj.file.name);
        xhr.setRequestHeader('X-ApiKey', '');
        xhr.setRequestHeader('Authorization', 'Bearer ' + $rootScope.currentUser.token);
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
