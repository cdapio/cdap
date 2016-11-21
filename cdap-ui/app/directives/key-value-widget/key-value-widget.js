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
  .directive('keyValueWidget', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '=',
        isDropdown: '='
      },
      templateUrl: 'key-value-widget/key-value-widget.html',
      controller: KeyValueController,
      controllerAs: 'KeyValue',
      bindToController: true
    };
  });

function KeyValueController(myHelpers, $scope) {
  this.keyPlaceholder = myHelpers.objectQuery(this.config, 'widget-attributes', 'key-placeholder') || 'key';
  this.valuePlaceholder = myHelpers.objectQuery(this.config, 'widget-attributes', 'value-placeholder') || 'value';

  // initializing
  function initialize() {
    var map = this.model;
    this.properties = [];

    if (!map || Object.keys(map).length === 0) {
      this.properties.push({
        key: '',
        value: ''
      });
      return;
    }

    Object.keys(map).forEach((key) => {
      this.properties.push({
        key: key,
        value: map[key]
      });
    });
  }

  initialize.call(this);

  $scope.$watch('KeyValue.properties', function() {

    var map = {};

    angular.forEach(this.properties, function(p) {
      if(p.key.length > 0){
        map[p.key] = p.value;
      }
    });
    this.model = map;
  }.bind(this), true);

  this.addProperty = function() {
    this.properties.push({
      key: '',
      value: '',
      newField: 'add'
    });
  };

  this.removeProperty = function(property) {
    var index = this.properties.indexOf(property);
    this.properties.splice(index, 1);
  };

  this.enter = function (event, last) {
    if (last && event.keyCode === 13) {
      this.addProperty();
    }
  };
}
