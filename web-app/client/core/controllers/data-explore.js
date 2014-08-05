/*
 * Dataexplore Controller
 */

define([], function () {
  var url = "data/explore/queries";
	var Controller = Em.Controller.extend({

		load: function () {
		  var self = this;
		  C.hideAllTables = function(){
		    self.hideAllTables();
		  };

		  self.limit = 4;
		  self.offset = null;
		  self.direction = null;
      self.largestEver = null;

		  this.set('objArr', []);
		  this.fetchQueries();
		  this.interval = setInterval(function () {
        self.fetchQueries();
		  }, 5000);
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

            if(datasets.length == 1){
              self.selectDataset(datasets[0]);
            }
          });
		    });
		  });
		},

		toggleTable: function (obj, hideAllFirst, callback) {
      var self = this;
      hideAllFirst = typeof hideAllFirst !== 'undefined' ? hideAllFirst : true;
      if(hideAllFirst){
        self.hideAllTables(obj);
      }
      if(!obj.get('has_results') || !obj.get('is_active')) {
        return;
      }
      obj.set('isSelected', !obj.get('isSelected'));
      $("#" + obj.query_handle).slideToggle(200, function () {
        var objArr = self.get('objArr');
        objArr.forEach(function (entry) {
          if (!$("#" + obj.query_handle + " :visible")) {
            entry.set('isSelected', false);  
          }
        });
        if (typeof callback === "function") {
            callback();
        }
      });
    },

    hideAllTables: function (dontHide) {
      var objArr = this.get('objArr');
      for (var i=0; i<objArr.length; i++){
        var query = objArr[i];
        if(query === dontHide){
          continue;
        }
        if(query.get('isSelected')){
          this.toggleTable(query, false);
          query.set('isSelected', false);
        }
      }
    },

    hideTable: function (query, callback) {
      if(query.get('isSelected')){
        this.toggleTable(query, false, callback);
      } else if (typeof callback === "function") {
        callback();
      }
    },

    selectDataset: function (dataset) {
      this.set('selectedDataset', dataset);
      $("#query-injector-input").attr('placeholder','SELECT * FROM ' + dataset.name + ' LIMIT 5');
      this.set('placeholder', 'SELECT * FROM ' + dataset.name + ' LIMIT 5');
      var datasets = this.get('datasets');
      datasets.forEach(function (entry) {
        entry.set('isSelected', false);
      });
      dataset.set('isSelected', true);
    },

		unload: function () {},

    nextPage: function () {
      var self = this;
      self.offset = self.smallest;
      self.cursor = "next";

      self.fetchQueries(true);
    },

    prevPage: function () {
      var self = this;
      self.offset = self.largest;
      self.cursor = "prev";

      self.fetchQueries(true);
    },

		fetchQueries: function (clearLocalFirst) {
		  var self = this;

      if(self.largest == self.largestEver && self.cursor === "prev"){
        self.offset = null;
        self.cursor = null;
      }

		  var url = 'data/explore/queries';
      url += '?limit=' + self.limit;
		  if(self.offset){
		    url += '&offset=' + self.offset;
		  }
		  if(self.cursor){
        url += '&cursor=' + self.cursor;
      }
      this.HTTP.rest(url, function (queries, status) {

        if(queries.length == 0){
          if(self.cursor == "prev"){
            self.offset = null;
            self.cursor = null;
          }
          return;
        }
        if (queries.length < self.limit) {
          if(self.cursor == "prev"){
            self.offset = null;
            self.cursor = "next";
            self.fetchQueries();
            return;
          }
        }

        //Clear the local memory of the queries if the list of queries in the response is different than the local version (and our local version isn't empty)
        if(clearLocalFirst || (  self.get('objArr').length && (self.get('objArr')[0].get('query_handle') != queries[0].query_handle)  )){
          self.set('objArr', []);
        }
		    var objArr = self.get('objArr');

        queries.sort(function(a, b){
            if(a.timestamp < b.timestamp) return 1;
            if(a.timestamp > b.timestamp) return -1;
            return 0;
        });
        queries.forEach(function (query) {
          var existingObj = self.find(query.query_handle);
          if (!existingObj) {
            var newObj = Ember.Object.create(query);
            newObj.query_handle_hashed = "#" + newObj.query_handle;
            newObj.time_started = new Date(query.timestamp);
            newObj.time_started = newObj.time_started.toLocaleString();
//            newObj.time_started = newObj.time_started.toString().replace(/\ GMT.*/,'');

            existingObj = objArr.pushObject(newObj);
          } else if (   existingObj.get('status')      !== query.status
                     || existingObj.get('has_results') !== query.has_results
                     || existingObj.get('is_active')   !== query.is_active) {
              //Avoid updating the query object upon every response from the server.
              //Instead, only update the query object when attributes actually change.
              //Otherwise, emberjs will update the view (even when no attribute really changed) and the update can mess up any dynamic view (toggled tables).
              self.hideTable(existingObj, function () {
                existingObj.set('status', query.status);
                existingObj.set('has_results', query.has_results);
                existingObj.set('is_active', query.is_active);
              });
          }
          if (existingObj.get('status') === 'FINISHED' && existingObj.get('has_results') && existingObj.get('is_active')) {
            if (!existingObj.get('results')) {
              self.getPreview(existingObj);
              self.getSchema(existingObj);
            }
          }
        });


        if(objArr.length){
          self.largest = objArr[0].timestamp;
          self.smallest = objArr[objArr.length - 1].timestamp;
          if(self.largest > self.largestEver) {
            self.largestEver = self.largest;
          }
        }
      });
		},

    getPreview: function (query) {
      var self = this;
      var handle = query.get('query_handle');
      this.HTTP.post('rest/data/explore/queries/' + handle + '/preview', function (response, status) {
        if (status != 200) {
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
      this.hideTable(query, function () {
        query.set('is_active', false);
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
            C.Util.showWarning(' ' + response.message);
            return;
          }
          self.fetchQueries();
          self.largestEver += 1;
        }
      );
      self.hideAllTables();
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
