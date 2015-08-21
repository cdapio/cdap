angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceTemplatesListController', function ($scope, mySettings, $stateParams, $alert, myHelpers) {

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

        sources = myHelpers.objectQuery(response, $stateParams.nsadmin, 'ETLBatch', 'source');
        transforms = myHelpers.objectQuery(response, $stateParams.nsadmin, 'ETLBatch', 'transform');
        sinks = myHelpers.objectQuery(response, $stateParams.nsadmin, 'ETLBatch', 'sink');

        objectToArray(sources);
        objectToArray(transforms);
        objectToArray(sinks);

        sources = myHelpers.objectQuery(response, $stateParams.nsadmin, 'ETLRealtime', 'source');
        transforms = myHelpers.objectQuery(response, $stateParams.nsadmin, 'ETLRealtime', 'transform');
        sinks = myHelpers.objectQuery(response, $stateParams.nsadmin, 'ETLRealtime', 'sink');

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
          delete res[$stateParams.nsadmin][template.templateType][template.type][template.templateName];

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
