/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

class TrackerEntityController{
  constructor($state) {
    'ngInject';

    this.$state = $state;

    this.entityInfo = {
      name: 'Dataset',
      icon: 'icon-datasets'
    };

    this.showLineage = window.CaskCommon.ThemeHelper.Theme.showLineage;
  }

  goBack() {
    this.$state.go('tracker.detail.result', {
      namespace: this.$state.params.namespace,
      searchQuery: this.$state.params.searchTerm
    });
  }
}

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerEntityController', TrackerEntityController);
