angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceTemplatesListController', function ($scope, mySettings, $stateParams, $alert, myHelpers) {

    var vm = this;
    vm.list = [];

    function objectToArray(obj) {
      var arr = [];

      angular.forEach(obj, function (value) {
        arr.push(value);
      });

      return arr;
    }

    function processResult(response) {
      if (response) {
        vm.list = [];

        ['ETLBatch', 'ETLRealtime'].forEach(function (templateType) {
          ['source', 'transform', 'sink'].forEach(function (pluginType) {
            var obj = myHelpers.objectQuery(response, $stateParams.nsadmin, templateType, pluginType);
            var pluginArray = objectToArray(obj);
            vm.list = vm.list.concat(pluginArray);
          });
        });
      }
    }


    mySettings.get('pluginTemplates')
      .then(processResult);

    vm.isEmpty = function () {
      return vm.list.length === 0;
    };

    vm.delete = function (template) {
      mySettings.get('pluginTemplates')
        .then(function (res) {
          delete res[$stateParams.nsadmin][template.templateType][template.pluginType][template.pluginTemplate];

          processResult(res);

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
