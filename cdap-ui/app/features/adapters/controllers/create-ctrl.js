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
