/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

/**
 * caskJsonEdit
 *
 * adapted from https://gist.github.com/maxbates/11002270
 *
 * <textarea cask-json-edit="myObject" rows="8" class="form-control"></textarea>
 */

angular.module(PKG.name+'.commons').directive('caskJsonEdit',
function myJsonEditDirective () {
  return {
    restrict: 'A',
    require: 'ngModel',
    template: '<textarea ng-model="jsonEditing"></textarea>',
    replace : true,
    scope: {
      model: '=caskJsonEdit'
    },
    link: function (scope, element, attrs, ngModelCtrl) {

      //init
      setEditing(scope.model);

      //check for changes going out
      scope.$watch('jsonEditing', function (newval, oldval) {
        if (newval !== oldval) {
          if (isValidJson(newval)) {
            setValid();
            updateModel(newval);
          } else {
            setInvalid();
          }
        }
      }, true);

      //check for changes coming in
      scope.$watch('model', function (newval, oldval) {
        if (newval !== oldval) {
          setEditing(newval);
        }
      }, true);


      function setEditing (value) {
        scope.jsonEditing = angular.copy(json2string(value));
      }

      function updateModel (value) {
        scope.model = string2json(value);
      }

      function setValid() {
        ngModelCtrl.$setValidity('json', true);
      }

      function setInvalid () {
        ngModelCtrl.$setValidity('json', false);
      }

      function string2json(text) {
        try {
          return angular.fromJson(text);
        } catch (err) {
          setInvalid();
          return text;
        }
      }

      function json2string(obj) {
        // better than JSON.stringify(), because it formats + filters $$hashKey etc.
        // NOTE that this will remove all $-prefixed values
        return angular.toJson(obj, true);
      }

      function isValidJson(model) {
        var flag = true;
        try {
          angular.fromJson(model);
        } catch (err) {
          flag = false;
        }
        return flag;
      }

    }
  };
});
