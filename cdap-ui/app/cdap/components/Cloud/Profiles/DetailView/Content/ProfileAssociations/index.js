/*
 * Copyright © 2018 Cask Data, Inc.
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

import PropTypes from 'prop-types';
import React, {Component} from 'react';
import {MySearchApi} from 'api/search';
import {isNilOrEmpty, humanReadableDuration, objectQuery} from 'services/helpers';
import {GLOBALS} from 'services/global-constants';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import {
  ONEDAYMETRICKEY,
  OVERALLMETRICKEY,
  fetchAggregateProfileMetrics,
  getNodeHours
} from 'components/Cloud/Profiles/Store/ActionCreator';
import {Observable} from 'rxjs/Observable';
require('./ProfileAssociations.scss');

const PREFIX = 'features.Cloud.Profiles.DetailView';
const HEADERPREFIX = `${PREFIX}.Associations.Header`;


const HEADERS = [
  {
    label: T.translate(`${HEADERPREFIX}.name`),
    property: 'name'
  },
  {
    label: T.translate(`${HEADERPREFIX}.namespace`),
    property: 'namespace'
  },
  {
    label: T.translate(`${HEADERPREFIX}.created`),
    property: 'created'
  },
  {
    label: T.translate(`${HEADERPREFIX}.last24hrsruns`)
  },
  {
    label: T.translate(`${HEADERPREFIX}.totalruns`)
  },
  {
    label: T.translate(`${HEADERPREFIX}.last24hrsnodehr`)
  },
  {
    label: T.translate(`${HEADERPREFIX}.totalnodehr`)
  },
  {
    label: T.translate(`${HEADERPREFIX}.schedules`)
  },
  {
    label: T.translate(`${HEADERPREFIX}.triggers`)
  }
];

export default class ProfileAssociations extends Component {
  static propTypes = {
    profile: PropTypes.object.isRequired,
    namespace: PropTypes.string.isRequired
  };

  state = {
    associationsMap: {}
  };

  fetchMetricsForApp = (appid, metadata) => {
    let {namespace, profile} = this.props;
    let extraTags = {
      program: metadata.program,
      programtype: metadata.type,
      profile: `${profile.scope}:${profile.name}`,
      app: metadata.app,
      namespace: metadata.namespace
    };
    fetchAggregateProfileMetrics(namespace, profile, extraTags)
      .subscribe(
        metricsMap => {
          let {associationsMap} = this.state;
          Object.keys(metricsMap).forEach(metricKey => {
            associationsMap[appid].metadata[metricKey] = metricsMap[metricKey];
          });
          this.setState({
            associationsMap
          });
        },
        () => {
          return Observable.create(observer => {
            observer.next({
              [ONEDAYMETRICKEY]: {
                runs: '--',
                minutes: '--'
              },
              [OVERALLMETRICKEY]: {
                runs: '--',
                minutes: '--'
              }
            });
          });
        }
      );
  }

  componentDidMount() {
    let {namespace, profile} = this.props;
    let {scope} = profile;
    scope = scope.toLowerCase();
    let profileName = `profile:${scope}:${profile.name}`;
    let apiObservable$;
    if (namespace === 'system') {
      apiObservable$ = MySearchApi.searchSystem({
        query: profileName
      });
    } else {
      apiObservable$ = MySearchApi.search({
        namespace,
        query: profileName
      });
    }
    apiObservable$
      .subscribe(
        res => {
          let associationsMap = this.convertMetadataToAssociations(res.results);
          this.setState({
            associationsMap
          });
          // FIXME: We should probably look into batching this to one single call.
          Object.keys(associationsMap)
            .forEach(appid => {
              let {metadata} = associationsMap[appid];
              this.fetchMetricsForApp(appid, metadata);
            });
        },
        err => {
          console.log(err);
        }
      );
  }

  convertMetadataToAssociations = (metadata) => {
    let appsMap = {};
    metadata.forEach(m => {
      let existingEntry = appsMap[m.entityId.application];
      if (!existingEntry) {
        existingEntry = {
          name: m.entityId.application,
          namespace: m.entityId.namespace,
          schedules: [],
          triggers: []
        };
        appsMap[m.entityId.application] = existingEntry;
      }
      if (m.entityId.schedule) {
        // fixed name for time based schedule.
        if (m.entityId.schedule === GLOBALS.defaultScheduleId) {
          appsMap[m.entityId.application] = {
            ...existingEntry,
            schedules: [...(existingEntry.schedules), m.entityId]
          };
        } else {
          appsMap[m.entityId.application] = {
            ...existingEntry,
            triggers: [...(existingEntry.triggers), m.entityId]
          };
        }
      } else if (!isNilOrEmpty(m.entityId.type)) {
        appsMap[m.entityId.application] = {
          ...existingEntry,
          created: m.metadata.SYSTEM.properties['creation-time'],
          metadata: {
            type: m.entityId.type,
            program: m.entityId.program,
            app: m.entityId.application,
            namespace: m.entityId.namespace
          }
        };
      }
    });
    return appsMap;
  };
  renderGridHeader = () => {
    return (
      <div className="grid-header">
        <div className="grid-row sub-header">
          <div />
          <div />
          <div />
          <div />
          <div />
          <div className="sub-title">
            {T.translate(`${PREFIX}.profileUsage`, {
              profile: this.props.profile.name
            })}
          </div>
          <div />
          <div />
          <div />
        </div>
        <div className="grid-row">
          {
            HEADERS.map(header => {
              return (
                <strong>
                  {header.label}
                </strong>
              );
            })
          }
        </div>
      </div>
    );
  };

  renderGridBody = () => {
    let {associationsMap} = this.state;
    return (
      <div className="grid-body">
        {
          Object.keys(associationsMap).map(app => {
            let appObj = associationsMap[app];
            let onedayMetrics = objectQuery(appObj, 'metadata', ONEDAYMETRICKEY) || {};
            let overallMetrics = objectQuery(appObj, 'metadata', OVERALLMETRICKEY) || {};
            return (
              <div className="grid-row">
                <div>{appObj.name}</div>
                <div>{appObj.namespace}</div>
                <div>{humanReadableDuration((Date.now() - parseInt(appObj.created, 10)) / 1000, true)}</div>
                {/*
                  We should set the defaults in the metrics call but since it is not certain that we get metrics
                  for all the profiles all the time I have added the defaults here in the view
                  Ideally we should set the defaults when we create the map of profiles.

                  This is the minimal change for 5.0
                */}
                <div>{onedayMetrics.runs || '--'} </div>
                <div>{overallMetrics.runs || '--'}</div>
                <div>{getNodeHours(onedayMetrics.minutes || '--')}</div>
                <div>{getNodeHours(overallMetrics.minutes || '--')}</div>
                <div>{appObj.schedules.length}</div>
                <div>{appObj.triggers.length}</div>
              </div>
            );
          })
        }
      </div>
    );
  };

  render() {
    if (isNilOrEmpty(this.state.associationsMap)) {
      return (
        <div className="profile-associations empty">
          <IconSVG name="icon-info-circle" />
          <h6> This profile is not associated with any schedules or triggers </h6>
        </div>
      );
    }
    return (
      <div className="profile-associations">
        <div className="grid grid-container">
          {this.renderGridHeader()}
          {this.renderGridBody()}
        </div>
      </div>
    );
  }
}
