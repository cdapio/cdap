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

class TrackerResultsController {
  constructor($state, myTrackerApi, $scope, myHelpers) {
    this.$state = $state;
    this.$scope = $scope;
    this.myTrackerApi = myTrackerApi;
    this.myHelpers = myHelpers;

    this.loading = false;
    this.entitiesShowAllButton = false;
    this.metadataShowAllButton = false;
    this.currentPage = 1;
    this.fullResults = [];
    this.searchResults = [];
    this.sortByOptions = [
      {
        name: 'Oldest First',
        sort: 'createDate'
      },
      {
        name: 'Newest First',
        sort: '-createDate'
      },
      {
        name: 'A → Z',
        sort: 'name'
      },
      {
        name: 'Z → A',
        sort: '-name'
      },
      {
        name: 'Highest Score',
        sort: '-meter'
      },
      {
        name: 'Lowest Score',
        sort: 'meter'
      }
    ];

    this.sortBy = this.sortByOptions[0];

    this.entityFiltersList = [
      {
        name: 'Datasets',
        isActive: true,
        isHover: false,
        filter: 'Dataset',
        count: 0
      },
      {
        name: 'Streams',
        isActive: true,
        isHover: false,
        filter: 'Stream',
        count: 0
      },
      {
        name: 'Stream Views',
        isActive: true,
        isHover: false,
        filter: 'Stream View',
        count: 0
      }
    ];

    this.metadataFiltersList = [
      {
        name: 'Name',
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: 'Description',
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: 'User Tags',
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: 'System Tags',
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: 'User Properties',
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: 'System Properties',
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: 'Schema',
        isActive: true,
        isHover: false,
        count: 0
      }
    ];

    this.fetchResults();
  }

  fetchResults () {
    this.loading = true;

    let params = {
      namespace: this.$state.params.namespace,
      query: this.$state.params.searchQuery,
      scope: this.$scope
    };

    if (params.query === '*') {
      params.sort = 'creation-time desc';
      params.numCursors = 10;
    } else if (params.query.charAt(params.query.length - 1) !== '*') {
      params.query = params.query + '*';
    }

    this.myTrackerApi.search(params)
      .$promise
      .then( (res) => {
        this.fullResults = res.results.map(this.parseResult.bind(this));
        this.searchResults = angular.copy(this.fullResults);
        if (this.searchResults.length) {
          this.fetchTruthMeter();
        }
        this.loading = false;
      }, (err) => {
        console.log('error', err);
        this.loading = false;
      });
  }
  parseResult (entity) {
    let obj = {};
    if (entity.entityId.type === 'datasetinstance') {
      angular.extend(obj, {
        name: entity.entityId.id.instanceId,
        type: 'Dataset',
        entityTypeState: 'datasets',
        icon: 'icon-datasets',
        description: entity.metadata.SYSTEM.properties.description || 'No description provided for this Dataset.',
        createDate: entity.metadata.SYSTEM.properties['creation-time'],
        datasetType: entity.metadata.SYSTEM.properties.type
      });
      if(entity.metadata.SYSTEM.tags.indexOf('explore') !== -1) {
        obj.datasetExplorable = true;
      }
      obj.queryFound = this.findQueries(entity, obj);
      this.entityFiltersList[0].count++;
    } else if (entity.entityId.type === 'stream') {
      angular.extend(obj, {
        name: entity.entityId.id.streamName,
        type: 'Stream',
        entityTypeState: 'streams',
        icon: 'icon-streams',
        description: entity.metadata.SYSTEM.properties.description || 'No description provided for this Stream.',
        createDate: entity.metadata.SYSTEM.properties['creation-time']
      });
      obj.queryFound = this.findQueries(entity, obj);
      this.entityFiltersList[1].count++;
    } else if (entity.entityId.type === 'view') {
      // THIS SECTION NEEDS TO BE UPDATED
      angular.extend(obj, {
        name: entity.entityId.id.id,
        type: 'Stream View',
        entityTypeState: 'views:' + entity.entityId.id.stream.streamName,
        icon: 'icon-streams',
        description: entity.metadata.SYSTEM.properties.description || 'No description provided for this Stream View.',
        createDate: entity.metadata.SYSTEM.properties['creation-time']
      });
      obj.queryFound = this.findQueries(entity, obj);
      this.entityFiltersList[2].count++;
    }
    return obj;
  }

  findQueries(entity, parsedEntity) {

    // Removing special characters from search query
    let replaceRegex = new RegExp('[^a-zA-Z0-9]', 'g');
    let searchTerm = this.$state.params.searchQuery.replace(replaceRegex, '');

    let regex = new RegExp(searchTerm, 'ig');

    let foundIn = [];

    if (parsedEntity.name.search(regex) > -1) {
      foundIn.push('Name');
      this.metadataFiltersList[0].count++;
    }
    if (entity.metadata.SYSTEM.properties.description && entity.metadata.SYSTEM.properties.description.search(regex) > -1) {
      foundIn.push('Description');
      this.metadataFiltersList[1].count++;
    }

    let userTags = this.myHelpers.objectQuery(entity, 'metadata', 'USER', 'tags') || '';
    userTags = userTags.toString();
    let systemTags = this.myHelpers.objectQuery(entity, 'metadata', 'SYSTEM', 'tags') || '';
    systemTags = systemTags.toString();

    if (userTags.search(regex) > -1) {
      foundIn.push('User Tags');
      this.metadataFiltersList[2].count++;
    }
    if (systemTags.search(regex) > -1) {
      foundIn.push('System Tags');
      this.metadataFiltersList[3].count++;
    }

    let userProperties = this.myHelpers.objectQuery(entity, 'metadata', 'USER', 'properties') || {};
    userProperties = JSON.stringify(userProperties);
    let systemProperties = this.myHelpers.objectQuery(entity, 'metadata', 'SYSTEM', 'properties') || {};
    systemProperties = JSON.stringify(systemProperties);

    if (userProperties.search(regex) > -1) {
      foundIn.push('User Properties');
      this.metadataFiltersList[4].count++;
    }
    if (systemProperties.search(regex) > -1) {
      foundIn.push('System Properties');
      this.metadataFiltersList[5].count++;
    }

    let schema = this.myHelpers.objectQuery(entity, 'metadata', 'SYSTEM', 'properties', 'schema') || '';

    if (schema.search(regex) > -1) {
      foundIn.push('Schema');
      this.metadataFiltersList[6].count++;
    }

    return foundIn;
  }

  onlyFilter(event, filter, filterType) {
    event.preventDefault();

    let filterObj = [];
    if (filterType === 'ENTITIES') {
      filterObj = this.entityFiltersList;
    } else if (filterType === 'METADATA') {
      filterObj = this.metadataFiltersList;
    }

    angular.forEach(filterObj, (entity) => {
      entity.isActive = entity.name === filter.name ? true : false;
    });

    this.filterResults();
  }

  filterResults() {
    let filter = [];
    angular.forEach(this.entityFiltersList, (entity) => {
      if (entity.isActive) { filter.push(entity.filter); }
    });

    let entitySearchResults = this.fullResults.filter( (result) => {
      return filter.indexOf(result.type) > -1 ? true : false;
    });


    let metadataFilter = [];
    angular.forEach(this.metadataFiltersList, (metadata) => {
      if (metadata.isActive) { metadataFilter.push(metadata.name); }
    });

    this.searchResults = entitySearchResults.filter( (result) => {
      return _.intersection(metadataFilter, result.queryFound).length > 0;
    });
  }

  showAll (filterType) {
    let filterArr = [];
    if (filterType === 'ENTITIES') {
      filterArr = this.entityFiltersList;
    } else if (filterType === 'METADATA') {
      filterArr = this.metadataFiltersList;
    }

    angular.forEach(filterArr, (filter) => {
      filter.isActive = true;
    });

    this.filterResults();
  }

  evaluateShowResultCount() {
    let lowerLimit = (this.currentPage - 1) * 10 + 1;
    let upperLimit = (this.currentPage - 1) * 10 + 10;

    upperLimit = upperLimit > this.searchResults.length ? this.searchResults.length : upperLimit;

    return this.searchResults.length === 0 ? '0' : lowerLimit + '-' + upperLimit;
  }

  fetchTruthMeter() {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };

    let streamsList = [];
    let datasetsList = [];

    angular.forEach(this.fullResults, (entity) => {
      if (entity.type === 'Stream') {
        streamsList.push(entity.name);
      } else if (entity.type === 'Dataset') {
        datasetsList.push(entity.name);
      }
    });

    this.myTrackerApi.getTruthMeter(params, {
      streams: streamsList,
      datasets: datasetsList
    })
      .$promise
      .then((response) => {
        if (!response) { return; }
        this.truthMeterMap = response;

        angular.forEach(this.fullResults, (entity) => {
          if (!response[entity.entityTypeState]) { return; }
          entity.meter = response[entity.entityTypeState][entity.name];
        });
        this.filterResults();
      }, (err) => {
        console.log('error', err);
      });
  }
}

TrackerResultsController.$inject = ['$state', 'myTrackerApi', '$scope', 'myHelpers'];

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerResultsController', TrackerResultsController);
