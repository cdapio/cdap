/*
 * Dataexplore Controller
 */

define([], function () {
  var url = "data/explore/queries";
	var Controller = Em.Controller.extend({

		load: function () {
		  var self = this;

		  self.limit = 4;
		  self.offset = null;
		  self.direction = null;

      self.largest = -1;
      self.smallest = -1;
		  this.set('objArr', []);
		  this.fetchQueries();
		  this.interval = setInterval(function () {
//        self.fetchQueries();
		  }, 1000);
		  this.set('datasets', []);
		  this.loadDiscoverableDatasets();
		},


		loadDiscoverableDatasets: function () {
		  var self = this;
      var datasets = self.get('datasets');
		  self.HTTP.rest('data/datasets?meta=true&explorable=true', function (response) {
		    response.forEach(function (dataset) {
		      var name = dataset.hive_table;
          var shortName = dataset.spec.name.replace(/.*\./,'');
          self.HTTP.rest('data/explore/datasets/' + shortName + '/schema', function (response, status) {
            var schemaString = '[{"name":"col_name"},{"name":"data_type"}]'
            schema = jQuery.parseJSON(schemaString);
            var results = [];
            for(var key in response) {
              if(response.hasOwnProperty(key)){
                results.push({columns:[key, response[key]]});
              }
            }
            datasets.pushObject(Ember.Object.create({name:name, shortName:shortName, schema:schema, results:results}));
          });
		    });
		  });
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
      $("#query-injector-input").attr('placeholder','SELECT * FROM ' + dataset.name + ' LIMIT 5');
      var datasets = this.get('datasets');
      datasets.forEach(function (entry) {
        entry.set('isSelected', false);
      });
      dataset.set('isSelected', true);
    },

		unload: function () {},

    getLargestSmallest: function () {
      var self = this;
      var objArr = self.get('objArr');
        if(objArr.length){
          self.largest = objArr[0].timestamp;
          self.smallest = objArr[0].timestamp;
          objArr.forEach(function(query){
            if(query.timestamp > self.largest){
              self.largest = query.timestamp;
            }
            if(query.timestamp < self.smallest){
              self.smallest = query.timestamp;
            }
          });
        }
    },

    nextPage: function () {
      var self = this;
      self.getLargestSmallest();
      self.offset = self.smallest;
      self.cursor = "next";
      self.fetchQueries();
    },

    prevPage: function () {
      var self = this;
      self.getLargestSmallest();
      self.offset = self.largest;
      self.cursor = "prev";
      self.fetchQueries();
    },

		fetchQueries: function () {
		  var self = this;
		  var url = 'data/explore/queries';
		  if(self.limit){
		    url += '?limit=' + self.limit;
		  }
		  if(self.offset){
		    url += '&offset=' + self.offset;
		  }
		  if(self.cursor){
        url += '&cursor=' + self.cursor;
      }
      console.log(url);
      this.HTTP.rest(url, function (queries, status) {
        if(status != 200) { return console.log('error in fetchQueries in data-explore.js'); }

        if(queries.length == 0){
          return;
        }
        self.set('objArr', []);
		    var objArr = self.get('objArr');

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

          if (existingObj.get('status') === 'FINISHED' && existingObj.get('has_results') && existingObj.get('is_active')) {
            if (!existingObj.get('results')) {
              self.getPreview(existingObj);
              self.getSchema(existingObj);
            }
          }
        });

      });
		},

    getPreview: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      this.HTTP.post('rest/data/explore/queries/' + handle + '/preview', function (response, status) {
        if (status != 200) {
          console.log('Error in getPreview');
          query.set('results', []);
          return;
        }
        response = jQuery.parseJSON( response );
        query.set('results', response);
      });
    },

    downloadFile: function(filename, content) {
      var blob = new Blob([content]);
      var evt = document.createEvent("HTMLEvents");
      evt.initEvent("click");
      $("<a>", {
        download: filename,
        href: webkitURL.createObjectURL(blob)
      }).get(0).dispatchEvent(evt);
    },

    download: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      var url = 'rest/data/explore/queries/' + handle + '/download';

      var handle = query.get('query_handle');
      self.HTTP.post(url, function (response) {
        self.downloadFile('results_' + handle + '.txt', response);
      });
    },

    deleteQuery: function (query) {
      var handle = query.get('query_handle');
      this.HTTP.del('rest/data/explore/queries/' + handle);
    },

    getSchema: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      this.HTTP.rest('data/explore/queries/' + handle + '/schema', function (response) {
        query.set('schema', response);
      });
    },

    submitSQLQuery: function () {
      var self = this;
      var controller = this.get('controllers');
      var sqlString = controller.get("SQLQueryString") || $("#query-injector-input").attr('placeholder');
      this.HTTP.post('rest/data/explore/queries', {data: { "query": sqlString }},
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
