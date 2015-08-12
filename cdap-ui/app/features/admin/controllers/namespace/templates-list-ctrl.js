angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceTemplatesListController', function ($scope, mySettings, $stateParams, $alert) {

    var vm = this;

    mySettings.get('pluginTemplates')
      .then(function (res) {
        vm.list = res[$stateParams.nsadmin];
      });

    vm.delete = function (template) {
      delete template;
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
