/*
 * Dataexplore Controller
 */

define([], function () {
  var url = "data/explore/queries";
	var Controller = Em.Controller.extend({


		load: function () {
		  var self = this;
		  this.set('objArr', []);
		  this.set('numQueries', 0);
		  this.fetchQueries();
		  this.interval = setInterval(function () {
		    //self.getExploreStatus();
        self.fetchQueries();
		  }, 1000);
		  this.set('datasets', []);
		  this.loadDiscoverableDatasets();


      C.deleteQuery = function(handle){
        self.deleteQuery(Ember.Object.create({query_handle:handle}));
      };
      C.cancelQuery = function(handle){
        self.cancelQuery(Ember.Object.create({query_handle:handle}));
      };
      C.HTTP = self.HTTP;
		},

		loadDiscoverableDatasets: function () {
		  var self = this;
      var datasets = self.get('datasets');
		  //TODO: hit the endpoint that Julien will provide.
      var completed = 0;
      console.log(completed);
      self.HTTP.post('rest/data/queries', {data: { "query": "show tables" }},
          function (response) {
            response = jQuery.parseJSON( response );
            self.HTTP.post('rest/data/queries/' + response.handle + '/next', function (response) {
              response = jQuery.parseJSON( response );
              response.forEach(function(data){
                var name = data.columns[0];
                var shortName = name.replace(/.*_/,'');
                var dataset = Ember.Object.create({name:name, shortName:shortName});
                datasets.pushObject(dataset);
                self.HTTP.post('rest/data/queries', {data: { "query": "describe " + dataset.name }},
                    function (response) {

                      C.Util.threadSleep(200);
                      response = jQuery.parseJSON( response );
                      self.HTTP.post('rest/data/queries/' + response.handle + '/next', function (response) {
                        response = jQuery.parseJSON( response );
                        dataset.set('results', response);
                        console.log(++completed);
                      });
                      self.HTTP.rest('data/queries/' + response.handle + '/schema', function (response) {
                        dataset.set('schema', response);
                        console.log(++completed);
                      });

                    }
                );
              });
            });
          }
      );
		},

    showTable: function (obj) {
      var self = this;
      obj.set('isSelected', !obj.get('isSelected'));
      $("#" + obj.query_handle).slideToggle(200, function () {
        var objArr = self.get('objArr');
        objArr.forEach(function (entry) {
          if (!$("#" + obj.query_handle + " :visible")) {
            entry.set('isSelected', false);  
          }
        });
      });
    },

    selectDataset: function (dataset) {
      this.set('selectedDataset', dataset);
      var datasets = this.get('datasets');
      datasets.forEach(function (entry) {
        entry.set('isSelected', false);
      });
      dataset.set('isSelected', true);
    },

		unload: function () {
		},

		fetchQueries: function () {
		  var self = this;
		  var objArr = this.get('objArr');
		  C.clearQueries = function(){objArr.forEach(function(query){query.set('deleted',true)});};
		  if(C.debug){ //TODO: remove this block (is/was for debugging purposes only).
		    C.debug = false;
		    debugger;
		  }
      this.HTTP.rest('data/explore/queries', function (queries, status) {
        if(status != 200) {
          console.log('error in fetchQueries in data-explore.js');
          return;
        }
        self.set('numQueries', queries.length);

        objArr.forEach(function(query){
          query.set('inList', false);
        });
        queries.forEach(function (query) {
          var existingObj = self.find(query.query_handle);
          if (!existingObj) {
            var newObj = Ember.Object.create(query);
            newObj.query_handle_hashed = "#" + newObj.query_handle;
            existingObj = objArr.pushObject(newObj);
          } else {
            existingObj.set('status', query.status);
            existingObj.set('has_results', query.has_results);
            existingObj.set('is_active', query.is_active);
          }

          existingObj.set('inList', true);
          if (existingObj.get('status') === 'FINISHED' && existingObj.get('has_results')) {
            if (!existingObj.get('results')) {
              self.getResults(existingObj);
              self.getSchema(existingObj);
            }
          }
        });
        objArr.forEach(function(query){
          query.set('deleted', !query.get('inList'));
        });
      });
		},

    getExploreStatus: function () {
      var self = this;
      this.HTTP.rest('explore/status', function () {
        //todo: set explore status in the view, if thats what is agreed upon for the view.
      });
    },

    getResults: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      this.HTTP.post('rest/data/queries/' + handle + '/next', function (response) {
        query.set('downloadableResults', "data:text/plain;charset=UTF-8," + response);
        query.set('downloadName', "results_" + handle + ".txt");
        response = jQuery.parseJSON( response );
        query.set('results', response);
      });
    },

    cancelQuery: function (query) {
      var handle = query.get('query_handle');
      this.HTTP.post('rest/data/queries/' + handle + '/cancel');
    },
    deleteQuery: function (query) {
      var handle = query.get('query_handle');
      this.HTTP.del('rest/data/queries/' + handle);
    },

    getSchema: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      this.HTTP.rest('data/queries/' + handle + '/schema', function (response) {
        query.set('schema', response);
      });
    },

    submitSQLQuery: function () {
      var self = this;
      var controller = this.get('controllers');
      var sqlString = controller.get("SQLQueryString");
      this.HTTP.post('rest/data/queries', {data: { "query": sqlString }},
        function (response, status) {
          if(status != 200) {
            C.Util.showWarning(response.error + ' : ' + response.message);
            console.log('error in submitSQLQuery in data-explore.js');
            return;
          }
          self.fetchQueries();
        }
      );
    },

    find: function (query_handle) {
      var objArr = this.get('objArr');
      for (var i=0; i<objArr.length; i++){
        if (query_handle === objArr[i].query_handle) {
          return objArr[i];
        }
      }
      return false;
    },

	});

	Controller.reopenClass({
		type: 'DataExplore',
		kind: 'Controller'
	});

	return Controller;

});
