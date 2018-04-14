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

import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {MyCloudApi} from 'api/cloud';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {Link} from 'react-router-dom';
import T from 'i18n-react';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import LoadingSVG from 'components/LoadingSVG';
import orderBy from 'lodash/orderBy';
import ViewAllLabel from 'components/ViewAllLabel';
require('./ListView.scss');

const PREFIX = 'features.Cloud.Profiles.ListView';

const PROFILES_TABLE_HEADERS = [
  {
    label: ''
  },
  {
    property: 'name',
    label: T.translate(`${PREFIX}.profileName`)
  },
  {
    property: (profile) => (profile.provisioner.name),
    label: T.translate(`${PREFIX}.provisioner`)
  },
  {
    property: 'scope',
    label: T.translate('commons.scope')
  },
  {
    property: 'pipelines',
    label: T.translate('commons.pipelines')
  },
  {
    property: 'last24HrRuns',
    label: T.translate(`${PREFIX}.last24HrRuns`)
  },
  {
    property: 'last24HrNodeHr',
    label: T.translate(`${PREFIX}.last24HrNodeHr`)
  },
  {
    property: 'totalNodeHr',
    label: T.translate(`${PREFIX}.totalNodeHr`)
  },
  {
    property: 'schedules',
    label: T.translate(`${PREFIX}.schedules`)
  },
  {
    property: 'triggers',
    label: T.translate(`${PREFIX}.triggers`)
  },
  {
    label: ''
  },
  {
    label: ''
  }
];

const SORT_METHODS = {
  asc: 'asc',
  desc: 'desc'
};

export default class ProfilesListView extends Component {
  state = {
    profiles: [],
    error: null,
    loading: true,
    viewAll: false,
    sortMethod: SORT_METHODS.asc,
    sortColumn: PROFILES_TABLE_HEADERS[1].property,
  };

  static propTypes = {
    namespace: PropTypes.string.isRequired,
    onChange: PropTypes.func
  };

  static defaultProps = {
    namespace: getCurrentNamespace()
  };

  componentDidMount() {
    MyCloudApi.list({
      namespace: this.props.namespace
    })
    .subscribe(
      profiles => {
        this.setState({
          profiles,
          loading: false
        }, () => {
          if (typeof this.props.onChange === 'function') {
            this.props.onChange(profiles);
          }
        });
      },
      err => {
        this.setState({
          error: err,
          loading: false
        });
      }
    );
  }

  toggleViewAll = () => {
    this.setState({
      viewAll: !this.state.viewAll
    });
  }

  handleProfilesSort = (field) => {
    let newSortColumn, newSortMethod;
    if (this.state.sortColumn === field) {
      newSortColumn = this.state.sortColumn;
      newSortMethod = this.state.sortMethod === SORT_METHODS.asc ? SORT_METHODS.desc : SORT_METHODS.asc;
    } else {
      newSortColumn = field;
      newSortMethod = SORT_METHODS.asc;
    }

    this.setState({
      sortColumn: newSortColumn,
      sortMethod: newSortMethod,
      profiles: orderBy(this.state.profiles, [newSortColumn], [newSortMethod])
    });
  };

  renderProfilesTable() {
    if (!this.state.profiles.length) {
      return (
        <div className="text-xs-center">
          {
            this.props.namespace === 'system' ?
              (
                <span>
                  {T.translate(`${PREFIX}.noProfilesSystem`)}
                  <Link to='/create-profile'>
                    {T.translate(`${PREFIX}.createOne`)}
                  </Link>
                </span>
              )
            :
              (
                <span>
                  {T.translate(`${PREFIX}.noProfiles`)}
                  <Link to={`/ns/${getCurrentNamespace()}/create-profile`}>
                    {T.translate(`${PREFIX}.createOne`)}
                  </Link>
                </span>
              )
          }

        </div>
      );
    }

    return (
      <div className="grid-wrapper">
        <div className="grid grid-container">
          {this.renderProfilesTableHeader()}
          {this.renderProfilesTableBody()}
        </div>
        <ViewAllLabel
          arrayToLimit={this.state.profiles}
          limit={10}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
      </div>
    );
  }

  renderSortIcon(field) {
    if (field !== this.state.sortColumn) {
      return null;
    }

    return (
      this.state.sortMethod === SORT_METHODS.asc ?
        <IconSVG name="icon-caret-down" />
      :
        <IconSVG name="icon-caret-up" />
    );
  }

  renderProfilesTableHeader() {
    return (
      <div className="grid-header">
        <div className="grid-row sub-header">
          <div />
          <div />
          <div />
          <div />
          <div />
          <div />
          <div className="sub-title">Pipeline Usage</div>
          <div/>
          <div className="sub-title">Associations</div>
          <div/>
          <div/>
          <div/>
        </div>
        <div className="grid-row">
          {
            PROFILES_TABLE_HEADERS.map((header, i) => {
              if (header.property) {
                return (
                  <strong
                    className={classnames("sortable-header", {"active": this.state.sortColumn === header.property})}
                    key={i}
                    onClick={this.handleProfilesSort.bind(this, header.property)}
                  >
                    <span>{header.label}</span>
                    {this.renderSortIcon(header.property)}
                  </strong>
                );
              }
              return (
                <strong key={i}>
                  {header.label}
                </strong>
              );
            })
          }
        </div>
      </div>
    );
  }

  renderProfilesTableBody() {
    let profiles = [...this.state.profiles];

    if (!this.state.viewAll && profiles.length > 10) {
      profiles = profiles.slice(0, 10);
    }

    return (
      <div className="grid-body">
        {
          profiles.map((profile, i) => {
            return (
              <div
                className="grid-row grid-link"
                key={i}
              >
                <div></div>
                <div title={profile.name}>
                  {profile.name}
                </div>
                <div>{profile.provisioner.name}</div>
                <div>{profile.scope}</div>
                <div />
                <div />
                <div />
                <div />
                <div />
                <div />
                <div />
                <div />
              </div>
            );
          })
        }
      </div>
    );
  }

  render() {
    if (this.state.loading) {
      return (
        <div className="text-xs-center">
          <LoadingSVG />
        </div>
      );
    }
    if (this.state.error) {
      return (
        <div className="text-danger">
          {JSON.stringify(this.state.error, null, 2)}
        </div>
      );
    }
    return (
      <div className="profiles-list-view">
        {this.renderProfilesTable()}
      </div>
    );
  }
}
