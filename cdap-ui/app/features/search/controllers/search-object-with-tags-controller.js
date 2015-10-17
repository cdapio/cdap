

class SearchObjectWithTagsController {
  constructor($stateParams, myTagsApi, caskFocusManager) {
    this.tag = $stateParams.tag;
    this.$stateParams = $stateParams;
    this.taggedObjects = [];
    this.myTagsApi = myTagsApi;
    this.gridsterOpts = {
      rowHeight: '40',
      columns: 12,
      minSizeX: 2,
      swapping: false,
      draggable: {
        enabled: false
      },
      resizable: {
        enabled: false
      }
    };
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
