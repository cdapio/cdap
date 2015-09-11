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

angular.module(PKG.name + '.feature.apps')
  .controller('AppListController', function CdapAppList($timeout, $scope, MyDataSource, myAppUploader, $alert, $state, MyOrderings, myAdapterApi, GLOBALS, myAdapterFactory) {
    this.MyOrderings = MyOrderings;
    this.apps = [];
    this.currentPage = 1;
    this.searchText = '';
    this.GLOBALS = GLOBALS;
    this.myAdapterFactory = myAdapterFactory;

    var data = new MyDataSource($scope);

    data.request({
      _cdapNsPath: '/apps/'
    })
      .then(function(apps) {
        this.apps = this.apps.concat(apps);
      }.bind(this));
    this.onFileSelected = myAppUploader.upload;

  });
