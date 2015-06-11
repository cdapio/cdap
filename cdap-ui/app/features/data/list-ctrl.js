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

    myDatasetApi.list(params)
      .$promise
      .then(function (res) {
        this.dataList = res
          .map(function(dataset) {
            dataset.dataType = 'Dataset';

            var datasetParams = {
              namespace: $state.params.namespace,
              datasetId: dataset.name,
              scope: $scope
            };

            myDatasetApi.programsList(datasetParams)
              .$promise
              .then(function (programs) {
                dataset.programs = programs;
              });

            return dataset;
          }.bind(this))
          .concat(this.dataList);
      }.bind(this));

    myStreamApi.list(params)
      .$promise
      .then(function(res) {
        this.dataList = res
          .map(function(stream) {
            stream.dataType = 'Stream';

            var streamParams = {
              namespace: $state.params.namespace,
              streamId: stream.name,
              scope: $scope
            };

            myStreamApi.programsList(streamParams)
              .$promise
              .then(function (programs) {
                stream.programs = programs;
              });

            return stream;
          }.bind(this))
          .concat(this.dataList);
      }.bind(this));

    this.goToDetail = function(data) {
      if (data.dataType === 'Dataset') {
        $state.go('datasets.detail.overview.status', {
          datasetId: data.name
        });
      } else if (data.dataType === 'Stream') {
        $state.go('streams.detail.overview.status', {
          streamId: data.name
        });
      }
      MyOrderings.dataClicked(data.name);
    };

  });
