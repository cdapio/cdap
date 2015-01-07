angular.module(PKG.name + '.services')
  .factory('fileUploader', function($q, $state) {
    function upload(fileObj){
      var deferred = $q.defer();
      var xhr = new XMLHttpRequest();
      xhr.upload.addEventListener('progress', function (e) {
        if (e.type === "progress") {
          console.log("Progress! ");
        }
      });
      var path = fileObj.path;
      xhr.open('POST', path, true);
      xhr.setRequestHeader("Content-type", "application/octet-stream");
      xhr.setRequestHeader("X-Archive-Name", fileObj.file.name);
      xhr.send(fileObj.file);
      xhr.onreadystatechange = function () {
        if (xhr.readyState === 4) {
          deferred.resolve();
        }
      };
      return deferred.promise;
    }
    return {
      upload: upload
    };
  });
