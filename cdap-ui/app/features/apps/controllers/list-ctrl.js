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

 class AppListController {
   constructor($scope, MyCDAPDataSource, myAppUploader, MyOrderings, GLOBALS, myHydratorFactory, getHydratorUrl) {
     this.getHydratorUrl = getHydratorUrl;
     this.MyOrderings = MyOrderings;
     this.apps = [];
     this.currentPage = 1;
     this.searchText = '';
     this.GLOBALS = GLOBALS;
     this.myHydratorFactory = myHydratorFactory;

     var data = new MyCDAPDataSource($scope);

     data.request({
       _cdapNsPath: '/apps/'
     })
       .then( (apps)=> {
         this.apps = this.apps.concat(apps);
       });
     this.onFileSelected = myAppUploader.upload;

   }
 }

 AppListController.$inject = ['$scope', 'MyCDAPDataSource', 'myAppUploader', 'MyOrderings', 'GLOBALS', 'myHydratorFactory', 'getHydratorUrl'];

angular.module(PKG.name + '.feature.apps')
  .controller('AppListController', AppListController);
