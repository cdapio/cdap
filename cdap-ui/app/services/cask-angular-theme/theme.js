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
 * caskTheme
 */

angular.module(PKG.name+'.services')

  .constant('CASK_THEME_EVENT', {
    changed: 'cask-theme-changed'
  })

  .provider('caskTheme', function CaskThemeProvider () {

    var THEME_LIST = ['default'];

    this.setThemes = function (t) {
      if(angular.isArray(t) && t.length) {
        THEME_LIST = t;
      }
    };

    this.$get = function ($localStorage, $rootScope, CASK_THEME_EVENT) {

      function Factory () {

        this.current = $localStorage.theme || THEME_LIST[0];

        this.set = function (theme) {
          if (THEME_LIST.indexOf(theme)!==-1) {
            this.current = theme;
            $localStorage.theme = theme;
            $rootScope.$broadcast(CASK_THEME_EVENT.changed, this.getClassName());
          }
        };

        this.list = function () {
          return THEME_LIST;
        };

        this.getClassName = function () {
          return 'theme-' + this.current;
        };

      }

      return new Factory();
    };

  });
