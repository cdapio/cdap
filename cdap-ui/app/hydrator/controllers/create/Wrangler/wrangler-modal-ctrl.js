/*
 * Copyright © 2016 Cask Data, Inc.
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

class WranglerModalController {
  constructor(rPlugin, $uibModalInstance, EventPipe) {
    this.node = rPlugin;
    this.$uibModalInstance = $uibModalInstance;
    this.EventPipe = EventPipe;

    this.applyToHydrator = this.applyToHydrator.bind(this);
  }

  applyToHydrator (properties) {
    this.node.plugin.properties.schema = properties.schema;
    this.node.plugin.properties.specification = properties.specification;

    this.EventPipe.emit('schema.import', properties.schema);

    this.$uibModalInstance.close();
  }
}


angular.module(PKG.name + '.feature.hydrator')
  .directive('wrangler', (reactDirective) => {
    return reactDirective(window.CaskCommon.Wrangler);
  })
  .controller('WranglerModalController', WranglerModalController);
