

class SearchObjectWithTagsController {
  constructor($stateParams) {
    this.tag = $stateParams.tag;
  }
}
SearchObjectWithTagsController.$inject = ['$stateParams'];

angular.module(`${PKG.name}.feature.search`)
  .controller('SearchObjectWithTagsController', SearchObjectWithTagsController);
