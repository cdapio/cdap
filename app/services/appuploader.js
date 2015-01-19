angular.module(PKG.name + '.services')
  .factory('myAppUploader', function(myFileUploader, $state, $alert) {
    function upload(files) {
      for (var i = 0; i < files.length; i++) {
        myFileUploader.upload({
          path: '/namespaces/' + $state.params.namespace + '/apps',
          file: files[i]
        })
          .then(success,error);
      }

      function success() {
        $alert({
          type: 'success',
          title: 'Upload success!',
          content: 'The application has been uploaded successfully!'
        });
        $state.reload();
      }

      // Independent xhr request. Failure case will not be handled by $rootScope.
      function error(err) {
        $alert({
          type: 'danger',
          title: 'Upload failed!',
          content: err || ''
        });
      }
    }

    return {
      upload: upload
    }
  });
