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

angular.module(PKG.name + '.commons')
  .directive('myMetadataTags', function () {

    return {
      restrict: 'E',
      controller: 'MetadataTagsController',
      controllerAs: 'MetadataController',
      scope: {
        params: '='
      },
      templateUrl: 'metadata-tags/metadata-tags.html',
    };
  })
  .controller('MetadataTagsController', function ($scope, myMetadataFactory) {
    this.metadataAddOpen = false;
    this.metadataTags = [];

    myMetadataFactory.getProgramMetadata($scope.params)
      .then(function (res) {
        this.metadataTags = res;
      }.bind(this));

    this.addMetadata = function () {
      myMetadataFactory.addProgramMetadata(this.tag, $scope.params)
        .then(function (res) {
          this.metadataTags = res;
          this.tag = '';
        }.bind(this));
    };

    this.deleteMetadata = function (tag) {
      myMetadataFactory.deleteProgramMetadata(tag, $scope.params)
        .then(function (res) {
          this.metadataTags = res;
        }.bind(this));
    };

  });
