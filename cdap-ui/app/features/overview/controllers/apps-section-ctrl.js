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

angular.module(PKG.name + '.feature.overview')
  .controller('AppsSectionCtrl', function(myAppUploader, myStreamApi, myDatasetApi, MyDataSource, MyOrderings, $scope, $state, myAdapterApi, GLOBALS, myAdapterFactory) {
    var dataSrc = new MyDataSource($scope);
    this.MyOrderings = MyOrderings;
    this.apps = [];
    this.GLOBALS = GLOBALS;
    this.myAdapterFactory = myAdapterFactory;

    this.dataList = [];
    dataSrc.request({
      _cdapNsPath: '/apps'
    })
      .then(function(res) {
        this.apps = this.apps.concat(res);
      }.bind(this));

    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    myDatasetApi.list(params)
      .$promise
      .then(function(res) {
        this.dataList = this.dataList.concat(res);
      }.bind(this));

    myStreamApi.list(params)
      .$promise
      .then(function(res) {
        if (angular.isArray(res) && res.length) {
          angular.forEach(res, function(r) {
            r.type = 'Stream';
          });

          this.dataList = this.dataList.concat(res);
        }
      }.bind(this));
    this.onFileSelected = myAppUploader.upload;
  });
