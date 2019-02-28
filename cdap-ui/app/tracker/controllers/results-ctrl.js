/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

const METADATA_FILTERS = {
  name: 'Name',
  description: 'Description',
  userTags: 'User tags',
  systemTags: 'System tags',
  userProperties: 'User properties',
  systemProperties: 'System properties',
  schema: 'Schema'
};

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
        name: 'Oldest first',
        sort: 'createDate'
      },
      {
        name: 'Newest first',
        sort: '-createDate'
      },
      {
        name: 'A → Z',
        sort: 'name'
      },
      {
        name: 'Z → A',
        sort: '-name'
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
    ];

    this.metadataFiltersList = [
      {
        name: METADATA_FILTERS.name,
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: METADATA_FILTERS.description,
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: METADATA_FILTERS.userTags,
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: METADATA_FILTERS.systemTags,
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: METADATA_FILTERS.userProperties,
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: METADATA_FILTERS.systemProperties,
        isActive: true,
        isHover: false,
        count: 0
      },
      {
        name: METADATA_FILTERS.schema,
        isActive: true,
        isHover: false,
        count: 0
      }
    ];

    this.numMetadataFiltersMatched = 0;
    this.fetchResults();
  }

  fetchResults () {
    this.loading = true;

    let params = {
      namespace: this.$state.params.namespace,
      query: this.$state.params.searchQuery,
      scope: this.$scope,
      responseFormat: 'v6',
    };

    if (params.query === '*') {
      params.sort = 'creation-time desc';
    } else if (params.query.charAt(params.query.length - 1) !== '*') {
      params.query = params.query + '*';
    }

    this.myTrackerApi.search(params)
      .$promise
      .then( (res) => {
        this.fullResults = res.results.map(this.parseResult.bind(this));
        this.numMetadataFiltersMatched = this.getMatchedFiltersCount();
        this.searchResults = angular.copy(this.fullResults);
        this.loading = false;
      }, (err) => {
        console.log('error', err);
        this.loading = false;
      });
  }
  parseResult (entityObj) {
    let obj = {};

    angular.extend(obj, {
      name: entityObj.entity.details.dataset,
      type: 'Dataset',
      entityTypeState: 'datasets',
      icon: 'icon-datasets'
    });

    let description = 'No description provided for this Dataset.',
        createDate = null,
        datasetType = null;

    angular.forEach(entityObj.metadata.properties, (property) => {
      switch (property.name) {
        case 'description':
          description = property.value;
          break;
        case 'creation-time':
          createDate = property.value;
          break;
        case 'type':
          datasetType = property.value;
          break;
      }
    });

    angular.extend(obj, {
      description: description,
      createDate: createDate,
      datasetType: datasetType,
    });

    if (entityObj.metadata.tags.find((tag) => tag.name === 'explore' && tag.scope === 'SYSTEM')) {
      obj.datasetExplorable = true;
    }

    obj.queryFound = this.findQueries(entityObj, obj);
    this.entityFiltersList[0].count++;

    return obj;
  }

  findQueries(entityObj, parsedEntity) {
    // Removing special characters from search query
    let replaceRegex = new RegExp('[^a-zA-Z0-9_-]', 'g');
    let searchTerm = this.$state.params.searchQuery.replace(replaceRegex, '');

    let regex = new RegExp(searchTerm, 'ig');

    let foundIn = [];

    // Name
    if (parsedEntity.name.search(regex) > -1) {
      foundIn.push(METADATA_FILTERS.name);
      this.metadataFiltersList[0].count++;
    }

    // Description
    const description = entityObj.metadata.properties.find((property) => property.name === 'decription' && property.scope === 'SYSTEM');
    if (description && description.value.search(regex) > -1) {
      foundIn.push(METADATA_FILTERS.description);
      this.metadataFiltersList[1].count++;
    }

    // Tags
    let userTags = entityObj.metadata.tags.filter((tag) => tag.scope === 'USER').map((tag) => tag.name);
    userTags = userTags.toString();
    let systemTags = entityObj.metadata.tags.filter((tag) => tag.scope === 'SYSTEM').map((tag) => tag.name);
    systemTags = systemTags.toString();

    if (userTags.search(regex) > -1) {
      foundIn.push(METADATA_FILTERS.userTags);
      this.metadataFiltersList[2].count++;
    }
    if (systemTags.search(regex) > -1) {
      foundIn.push(METADATA_FILTERS.systemTags);
      this.metadataFiltersList[3].count++;
    }

    // Properties
    function convertToObject(arr) {
      const returnObj = {};

      arr.forEach((pair) => {
        returnObj[pair.name] = pair.value;
      });

      return returnObj;
    }

    let userProperties = entityObj.metadata.properties.filter((property) => property.scope === 'USER');
    userProperties = JSON.stringify(convertToObject(userProperties));
    let systemProperties = entityObj.metadata.properties.filter((property) => property.scope === 'SYSTEM' && property.name !== 'schema');
    systemProperties = JSON.stringify(convertToObject(systemProperties));

    if (userProperties.search(regex) > -1) {
      foundIn.push(METADATA_FILTERS.userProperties);
      this.metadataFiltersList[4].count++;
    }
    if (systemProperties.search(regex) > -1) {
      foundIn.push(METADATA_FILTERS.systemProperties);
      this.metadataFiltersList[5].count++;
    }

    // Schema
    const schema = entityObj.metadata.properties.find((property) => property.name === 'schema' && property.scope === 'SYSTEM');
    if (schema && schema.value.search(regex) > -1) {
      foundIn.push(METADATA_FILTERS.schema);
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
      if (result.queryFound.length === 0) {
        return true;
      }
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

  getMatchedFiltersCount() {
    let metadataFilterCount = 0;
    angular.forEach(this.metadataFiltersList, (metadata) => {
      if (metadata.count > 0) { metadataFilterCount++; }
    });
    return metadataFilterCount;
  }
}

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerResultsController', TrackerResultsController);
