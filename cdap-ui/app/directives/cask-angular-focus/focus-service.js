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
 * caskFocusManager
 * watched by the caskFocus directive, this service can be called
 *  from a controller to trigger focus() events, presumably on form inputs
 * @return {Object}  with "focus" and "select" methods
 */

angular.module(PKG.name+'.commons').service('caskFocusManager',
function caskFocusManagerService ($rootScope, $log, $timeout) {

  var last = null;

  this.is = $rootScope.$new(true);

  function set (k, v) {
    var scope = this.is;
    $timeout(function() {
      $log.log('[caskFocusManager]', v, k);
      scope[last] = false;
      var o = {};
      o[v] = Date.now();
      scope[k] = o;
      last = k;
    });
  }

  /**
   * triggers focus() on element with cask-focus = k
   * @param  {String} k
   */
  this.focus = function(k) {
    set.call(this, k, 'focus');
  };

  /**
   * triggers select() on element with cask-focus = k
   * @param  {String} k
   */
  this.select = function(k) {
    set.call(this, k, 'select');
  };

});
