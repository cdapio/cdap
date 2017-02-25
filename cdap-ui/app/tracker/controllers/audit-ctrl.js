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

class TrackerAuditController {
  constructor($state, $scope, myTrackerApi, myAlertOnValium, $uibModal) {

    this.$state = $state;
    this.$scope = $scope;
    this.myTrackerApi = myTrackerApi;
    this.myAlertOnValium = myAlertOnValium;
    this.$uibModal = $uibModal;

    this.timeRangeOptions = [
      {
        label: 'Last 7 days',
        start: 'now-7d',
        end: 'now'
      },
      {
        label: 'Last 14 days',
        start: 'now-14d',
        end: 'now'
      },
      {
        label: 'Last month',
        start: 'now-30d',
        end: 'now'
      },
      {
        label: 'Last 6 months',
        start: 'now-180d',
        end: 'now'
      },
      {
        label: 'Last 12 months',
        start: 'now-365d',
        end: 'now'
      }
    ];

    this.timeRange = {
      start: $state.params.start || 'now-7d',
      end: $state.params.end || 'now'
    };

    this.customTimeRange = {
      startTime: null,
      endTime: null
    };

    this.enableTrackerLoading = false;

    this.selectedTimeRange = this.findTimeRange();

    this.currentPage = 1;
    this.auditLogs = [];
    this.fetchAuditLogs(this.currentPage);
  }

  findTimeRange() {
    let match = this.timeRangeOptions.filter( (option) => {
      return option.start === this.timeRange.start && option.end === this.timeRange.end;
    });

    if (match.length === 0) {
      this.isCustom = true;
      this.customTimeRange.startTime = new Date(parseInt(this.$state.params.start, 10) * 1000);
      this.customTimeRange.endTime = new Date(parseInt(this.$state.params.end, 10) * 1000);
    }

    return match.length > 0 ? match[0] : { label: 'Custom' };
  }

  goToCustomTimeRangeEntityDetailView() {
    let startTime = parseInt(this.customTimeRange.startTime.valueOf() / 1000, 10);
    let endTime = parseInt(this.customTimeRange.endTime.valueOf() / 1000, 10);

    this.$state.go('tracker.detail.entity.audit', { start: startTime, end: endTime });
  }

  selectCustom() {
    this.isCustom = true;
    this.selectedTimeRange.label = 'Custom';
  }

  fetchAuditLogs(currentPage) {
    this.enableTrackerLoading = false;
    let params = {
      namespace: this.$state.params.namespace,
      entityType: this.$state.params.entityType === 'streams' ? 'stream' : 'dataset',
      entityId: this.$state.params.entityId,
      startTime: this.timeRange.start,
      endTime: this.timeRange.end,
      offset: (currentPage - 1) * 10,
      scope: this.$scope
    };

    this.myTrackerApi.getAuditLogs(params)
      .$promise
      .then((response) => {
        this.auditLogs = response;
      }, (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          content: err.data
        });
      });
  }

  isMetadataChanged(type, payload) {

    if (type !== 'METADATA_CHANGE') {
      return false;
    }

    for (let key in payload) {

      if (payload.hasOwnProperty(key)) {

        if (_.get(payload[key], 'SYSTEM.tags') && _.get(payload[key], 'SYSTEM.tags').length > 0 ) {
          return true;
        }
        if (_.get(payload[key], 'USER.tags') && _.get(payload[key], 'USER.tags').length > 0 ) {
          return true;
        }
        if (_.get(payload[key], 'USER.properties') && !_.isEmpty(_.get(payload, 'USER.properties'))) {
          return true;
        }
      }
    }

    return false;
  }

  showMetadataChangeInfo(logData) {
    this.$uibModal.open({
      templateUrl: '/assets/features/tracker/templates/partial/metadata-change-info.html',
      size: 'lg',
      backdrop: true,
      keyboard: true,
      windowTopClass: 'tracker-modal metadata-info-modal',
      controller: metadataChangeInformation,
      controllerAs: 'MetadataInfo',
      resolve: {
        logDataObj: () => {
          return logData;
        }
      }
    });
  }
}

function metadataChangeInformation(logDataObj, avsc, SchemaHelper) {
  'ngInject';

  let allFormattedData = {},
    tags= {},
    schema= {},
    properties = {};

  let getSchemaArray = (schemaToParse) => {
    if (!schemaToParse) {
      return [];
    }

    let schemArr = [], parsed;

    try {
      parsed = avsc.parse(schemaToParse, { wrapUnions: true });
    } catch (e) {
      console.log('Error', e);
    }

    parsed.getFields().map((field) => {
      let type = field.getType();
      let partialObj = SchemaHelper.parseType(type);
      return Object.assign({}, partialObj, {
        id: uuid.v4(),
        name: field.getName()
      });
    }).filter((obj) => {
      if (obj) {
        schemArr.push(obj.name + ' ' + obj.displayType);
      }
    });

    return schemArr;
  };

  let setTags = (userType) => {
    tags.previousValues = logDataObj.previous[userType].tags;
    tags.addedValues = logDataObj.additions[userType].tags;
    tags.deletedValues = logDataObj.deletions[userType].tags;
  };

  if (logDataObj.previous.USER) {
    setTags('USER');

    properties.previousValues = _.map(logDataObj.previous.USER.properties, (value, key) => {
      return key + ':' + value;
    });
    properties.addedValues = _.map(logDataObj.additions.USER.properties, (value, key) => {
      return key + ':' + value;
    });
    properties.deletedValues = _.map(logDataObj.deletions.USER.properties, (value, key) => {
      return key + ':' + value;
    });
  }

  if (logDataObj.previous.SYSTEM) {
    setTags('SYSTEM');

    schema.previousValues = getSchemaArray(logDataObj.previous.SYSTEM.properties.schema);
    schema.addedValues = getSchemaArray(logDataObj.additions.SYSTEM.properties.schema);
    schema.deletedValues = getSchemaArray(logDataObj.deletions.SYSTEM.properties.schema);
  }

  allFormattedData.tags = tags;
  allFormattedData.schema = schema;
  allFormattedData.properties = properties;

  this.formattedData = allFormattedData;
}

TrackerAuditController.$inject = ['$state', '$scope', 'myTrackerApi', 'myAlertOnValium', '$uibModal'];

angular.module(PKG.name + '.feature.tracker')
.controller('TrackerAuditController', TrackerAuditController);

