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

class SearchObjectWithTagsController {
  constructor($stateParams, myTagsApi, caskFocusManager) {
    this.tag = $stateParams.tag;
    this.$stateParams = $stateParams;
    this.taggedObjects = [];
    this.myTagsApi = myTagsApi;
    caskFocusManager.select('searchObjectWithTags');
    this.fetchAssociatedObjects();
  }

  fetchAssociatedObjects() {
    let params = {
      namespaceId: this.$stateParams.namespace,
      query: `tags:${this.tag}*`
    };
    this.myTagsApi
      .searchTags(params)
      .$promise
      .then(
        (taggedObjects) => {
          taggedObjects.forEach( (tObject) => {
            this.parseTaggedObject(tObject);
          });
        }
      );
  }
  parseTaggedObject (tObject) {
    let type = tObject.entityId.type;
    let entityObj = tObject.entityId.id;
    var modifiedTObject = {
      sizeX: 2,
      sizeY: 1,
      row: null,
      col: null
    };

    switch (type) {
      case 'stream':
        angular.extend(modifiedTObject, {
          namespaceId: entityObj.namespace.id,
          streamId: entityObj.streamName,
          type: type,
          icon: 'icon-app'
        });
        break;
      case 'datasetinstance':
        angular.extend(modifiedTObject, {
          namespaceId: entityObj.namespace.id,
          datasetId: entityObj.instanceId,
          type: type,
          icon: 'icon-datasets'
        });
        break;
      case 'application':
        angular.extend(modifiedTObject, {
          namespaceId: entityObj.namespace.id,
          appId: entityObj.applicationId,
          type: type,
          icon: 'icon-app'
        });
        break;
      case 'program':

        angular.extend(modifiedTObject, {
          namespaceId: entityObj.application.namespace.id,
          appId: entityObj.application.applicationId,
          programType: entityObj.type,
          programId: entityObj.id,
          type: type,
          icon: (entityObj.type.toLowerCase() === 'flow'? 'icon-tigon': 'icon-' + entityObj.type.toLowerCase())
        });
        break;
    }
    this.taggedObjects.push(modifiedTObject);
  }
}
SearchObjectWithTagsController.$inject = ['$stateParams', 'myTagsApi', 'caskFocusManager'];

angular.module(`${PKG.name}.feature.search`)
  .controller('SearchObjectWithTagsController', SearchObjectWithTagsController);
