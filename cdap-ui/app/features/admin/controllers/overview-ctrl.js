/*
 * Copyright © 2015 Cask Data, Inc.
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
  .controller('OverviewController', function ($scope, $state, myNamespace, MyDataSource, myLocalStorage, MY_CONFIG, myStreamApi, myDatasetApi) {
    var dataSrc = new MyDataSource($scope),
        PREFKEY = 'feature.admin.overview.welcomeIsHidden';

    myLocalStorage.get(PREFKEY)
      .then(function (v) {
        $scope.welcomeIsHidden = v;
      });

    $scope.hideWelcome = function () {
      myLocalStorage.set(PREFKEY, true);
      $scope.welcomeIsHidden = true;
    };

    $scope.isEnterprise = MY_CONFIG.isEnterprise;

    // TODO: add dataset and stream counts per namespace
    myNamespace.getList()
      .then(function (list) {
        $scope.nsList = list;
        $scope.nsList.forEach(function (namespace) {
          console.log('namespace', namespace);
          getApps(namespace)
            .then(function (apps) {
              namespace.appsCount = apps.length;
            });
          getDatasets(namespace)
            .then(function (data) {
              namespace.datasetsCount = data.length;
            }, function error(err) {
              console.log('ERROR: Fetching Datasets failed ', err);
            });

          getStreams(namespace)
            .then(function (streams) {
              namespace.streamsCount = streams.length;
            });
        });
      });

    function getApps (namespace) {
      return dataSrc.request({
        _cdapPath: '/namespaces/' + namespace.name + '/apps'
      });
    }

    function getDatasets (namespace) {
      var params = {
        namespace: namespace.name,
        scope: $scope
      };
      return myDatasetApi.list(params).$promise;
    }

    function getStreams (namespace) {
      var params = {
        namespace: namespace.name,
        scope: $scope
      };
      return myStreamApi.list(params).$promise;
    }
  });
