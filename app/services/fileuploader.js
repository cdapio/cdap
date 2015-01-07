angular.module(PKG.name + '.services')
  .factory('myFileUploader', function($q, $window, cfpLoadingBar) {
    function upload(fileObj){
      var deferred = $q.defer();
      var xhr = new $window.XMLHttpRequest();
      xhr.upload.addEventListener('progress', function (e) {
        if (e.type === 'progress') {
          console.log('App Upload in progress!');
        }
      });
      var path = fileObj.path;
      xhr.open('POST', path, true);
      xhr.setRequestHeader('Content-type', 'application/octet-stream');
      xhr.setRequestHeader('X-Archive-Name', fileObj.file.name);
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
      return deferred.promise;
    }
    return {
      upload: upload
    };
  });
