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
import {isNilOrEmpty, humanReadableDuration} from 'services/helpers';
import {GLOBALS} from 'services/global-constants';
import IconSVG from 'components/IconSVG';

require('./ProfileAssociations.scss');
const HEADERS = [
  {
    label: 'Name',
    property: 'name'
  },
  {
    label: 'Namespace',
    property: 'namespace'
  },
  {
    label: 'Created',
    property: 'created'
  },
  {
    label: 'Last 24 hrs runs'
  },
  {
    label: 'Total runs'
  },
  {
    label: 'Last run node/hr'
  },
  {
    label: 'Total node/hr'
  },
  {
    label: 'Schedules'
  },
  {
    label: 'Triggers'
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

  componentDidMount() {
    let {namespace, profile} = this.props;
    MySearchApi
      .search({
        namespace,
        query: `profile:${namespace}.${profile.name}`
      })
      .subscribe(
        res => {
          this.setState({
            associationsMap: this.convertMetadataToAssociations(res.results)
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
          created: m.metadata.SYSTEM.properties['creation-time']
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
          <div className="sub-title">{this.props.profile.name} Usage</div>
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
            return (
              <div className="grid-row">
                <div>{appObj.name}</div>
                <div>{appObj.namespace}</div>
                <div>{humanReadableDuration((Date.now() - parseInt(appObj.created, 10)) / 1000, true)}</div>
                <div>--</div>
                <div>--</div>
                <div>--</div>
                <div>--</div>
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
