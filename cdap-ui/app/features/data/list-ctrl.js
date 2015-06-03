angular.module(PKG.name + '.feature.data')
  .controller('CdapDataListController', function($state, $scope, MyOrderings, myStreamApi, myDatasetApi) {
    this.MyOrderings = MyOrderings;
    this.dataList = [];
    this.currentPage = 1;
    this.searchText = '';
    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    myStreamApi.list(params)
      .$promise
      .then(function (res) {
        this.dataList = res
          .map(function(dataset) {
            dataset.dataType = 'Stream';
            return dataset;
          }.bind(this))
          .concat(this.dataList);
      }.bind(this));

    myDatasetApi.list(params)
      .$promise
      .then(function(res) {
        this.dataList = res
          .map(function(stream) {
            stream.dataType = 'Dataset';
            return stream;
          }.bind(this))
          .concat(this.dataList);
      }.bind(this));

    this.goToDetail = function(data) {
      if (data.dataType === 'Dataset') {
        $state.go('datasets.detail.overview', {
          datasetId: data.name
        });
      } else if (data.dataType === 'Stream') {
        $state.go('streams.detail.overview', {
          streamId: data.name
        });
      }
      MyOrderings.dataClicked(data.name);
    };

    this.goToList = function(data) {
      if (data.dataType === 'Dataset') {
        $state.go('datasets.list');
      } else if (data.dataType === 'Stream') {
        $state.go('streams.list');
      }
    };

  });
