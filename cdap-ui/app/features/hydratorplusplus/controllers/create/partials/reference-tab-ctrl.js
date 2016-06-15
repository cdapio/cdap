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

class HydratorPlusPlusReferenceTabCtrl {
  constructor(HydratorPlusPlusPluginConfigFactory, GLOBALS, myHelpers, $scope) {
    this.GLOBALS = GLOBALS;
    this.HydratorPlusPlusPluginConfigFactory = HydratorPlusPlusPluginConfigFactory;
    this.myHelpers = myHelpers;
    this.state = {};
    this.showContents($scope.node);
  }
  showContents(node) {
    if (!node.plugin) {
      this.state.docReference = this.GLOBALS.en.hydrator.studio.info['DEFAULT-REFERENCE'];
    } else {
      let key = `doc.${node.plugin.name}-${node.type}`;
      this.HydratorPlusPlusPluginConfigFactory.fetchDocJson(
        this.myHelpers.objectQuery(node, 'plugin', 'artifact', 'name'),
        this.myHelpers.objectQuery(node, 'plugin', 'artifact', 'version'),
        this.myHelpers.objectQuery(node, 'plugin', 'artifact', 'scope'),
        key
      ).then (
        (res) => {
          if (res[key]) {
            this.state.docReference = res[key];
          } else {
            this.state.docReference = this.GLOBALS.en.hydrator.studio.info['NO-REFERENCE'];
          }
        },
        () => this.state.docReference = this.GLOBALS.en.hydrator.studio.info['NO-REFERENCE']
      );
    }
  }
}

HydratorPlusPlusReferenceTabCtrl.$inject = ['HydratorPlusPlusPluginConfigFactory', 'GLOBALS', 'myHelpers', '$scope'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('HydratorPlusPlusReferenceTabCtrl', HydratorPlusPlusReferenceTabCtrl);
