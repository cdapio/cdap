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
  constructor(rPlugin, $uibModalInstance, EventPipe, $scope, myHelpers) {
    this.node = rPlugin;
    this.workspaceId = myHelpers.objectQuery(this.node, 'properties', 'workspaceId');
    this.$uibModalInstance = $uibModalInstance;
    this.EventPipe = EventPipe;
    this.onSubmit = this.onSubmit.bind(this);
    this.modalClosed = false;
    $scope.$on('modal.closing', (e, reason) => {
      if (reason === 'ADD_TO_PIPELINE') { return; }

      let shouldClose = confirm('Are you sure you want to exit Wrangler?');
      if (!shouldClose) { e.preventDefault(); }
    });
  }

  onSubmit ({workspaceId, directives, schema}) {
    if (this.modalClosed || !workspaceId) {
      return; // Modal already closed. Nothing to do anymore.
    }
    if (!directives || !schema) {
      this.node.properties.workspaceId = workspaceId;
      this.$uibModalInstance.close('ADD_TO_PIPELINE');
      this.modalClosed = true;
      return;
    }
    this.node.properties.schema = schema;
    this.node.properties.workspaceId = workspaceId;
    this.node.properties.directives = directives.join('\n');
    this.modalClosed = true;
    this.EventPipe.emit('schema.import', schema);

    this.$uibModalInstance.close('ADD_TO_PIPELINE');
  }
}


angular.module(PKG.name + '.commons')
  .directive('dataprep', (reactDirective) => {
    return reactDirective(window.CaskCommon.DataPrep);
  })
  .controller('WranglerModalController', WranglerModalController);
