angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceTemplatesListController', function ($scope, mySettings, $stateParams, $alert) {

    var vm = this;
    vm.list = {};


    mySettings.get('pluginTemplates')
      .then(function (res) {
        if (res) {
          vm.list = res[$stateParams.nsadmin];
        }
      });

    vm.isEmpty = function () {
      return Object.keys(vm.list).length === 0
    };

    vm.delete = function (template) {
      mySettings.get('pluginTemplates')
        .then(function (res) {
          delete res[$stateParams.nsadmin][template.templateName];

          vm.list = res[$stateParams.nsadmin];

          mySettings.set('pluginTemplates', res)
            .then(function () {
              $alert({
                type: 'success',
                content: 'Successfully deleted template'
              });
            });
        });
    };

  });
