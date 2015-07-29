angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterListController', function($scope, mySettings, $state, $alert, $timeout, myAlert, myHelpers) {
    $scope.drafts  = [];

    mySettings.get('adapterDrafts')
      .then(function(res) {
        if (res && Object.keys(res).length) {
          angular.forEach(res, function(value, key) {
            $scope.drafts.push({
              isdraft: true,
              name: key,
              template: myHelpers.objectQuery(value, 'config', 'metadata', 'type') || myHelpers.objectQuery(value, 'template'),
              status: '-',
              description: myHelpers.objectQuery(value, 'config', 'metadata', 'description') || myHelpers.objectQuery(value, 'description')
            });
          });
        }
      });

    $scope.deleteDraft = function(draftName) {
      mySettings.get('adapterDrafts')
        .then(function(res) {
          if (res[draftName]) {
            delete res[draftName];
          }
          return mySettings.set('adapterDrafts', res);
        })
        .then(
          function success() {
            $alert({
              type: 'success',
              content: 'Adapter draft ' + draftName + ' delete successfully'
            });
            $state.reload();
          },
          function error() {
            $alert({
              type: 'danger',
              content: 'Adapter draft ' + draftName + ' delete failed'
            });
            $state.reload();
          });
    }


  });
