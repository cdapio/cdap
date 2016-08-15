/*
 * Copyright © 2015-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceTemplatesListController', function ($scope, mySettings, $stateParams, myAlertOnValium, myHelpers, GLOBALS, myLoadingService) {

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

        [ GLOBALS.etlBatch, GLOBALS.etlRealtime].forEach(function (templateType) {
          [
            GLOBALS.pluginTypes[templateType].source,
            GLOBALS.pluginTypes[templateType].transform,
            GLOBALS.pluginTypes[templateType].sink
          ].forEach(function (pluginType) {
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
      myLoadingService.showLoadingIcon();
      mySettings.get('pluginTemplates')
        .then(
          function (res) {
            delete res[$stateParams.nsadmin][template.templateType][template.pluginType][template.pluginTemplate];

            processResult(res);

            mySettings.set('pluginTemplates', res)
              .then(
                function () {
                  myLoadingService.hideLoadingIcon();
                  myAlertOnValium.show({
                    type: 'success',
                    content: 'Successfully deleted template'
                  });
                },
                function error(err) {
                  myLoadingService.hideLoadingIcon();
                  myAlertOnValium.show({
                    type: 'danger',
                    title: 'Delete failed',
                    content: err
                  });
                }
              );
          },
          function error() {
            myLoadingService.hideLoadingIcon();
            myAlertOnValium.show({
              type: 'danger',
              title: 'User store acces failed'
            });
          }
        );
    };

  });
