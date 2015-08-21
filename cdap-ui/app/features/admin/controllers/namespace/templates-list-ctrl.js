angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceTemplatesListController', function ($scope, mySettings, $stateParams, $alert) {

    var vm = this;
    vm.list = [];

    function objectToArray(obj) {
      angular.forEach(obj, function (value) {
        vm.list.push(value);
      });
    }

    function processResult(response) {
      if (response) {
        vm.list = [];

        var sources, transforms, sinks;
        sources = response[$stateParams.nsadmin].source;
        transforms = response[$stateParams.nsadmin].transform;
        sinks = response[$stateParams.nsadmin].sink;

        objectToArray(sources);
        objectToArray(transforms);
        objectToArray(sinks);
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
          delete res[$stateParams.nsadmin][template.type][template.templateName];

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
