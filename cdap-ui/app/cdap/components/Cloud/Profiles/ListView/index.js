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
import {getCurrentNamespace} from 'services/NamespaceStore';
import {Link} from 'react-router-dom';
import T from 'i18n-react';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import LoadingSVG from 'components/LoadingSVG';
import orderBy from 'lodash/orderBy';
import ViewAllLabel from 'components/ViewAllLabel';
import ConfirmationModal from 'components/ConfirmationModal';
import ProfilesStore from 'components/Cloud/Profiles/Store';
import {getProfiles, deleteProfile, setError} from 'components/Cloud/Profiles/Store/ActionCreator';
import {connect, Provider} from 'react-redux';
import Alert from 'components/Alert';
import uuidV4 from 'uuid/v4';
import ActionsPopover from 'components/Cloud/Profiles/ActionsPopover';
import isEqual from 'lodash/isEqual';
import {MyCloudApi} from 'api/cloud';

require('./ListView.scss');

const PREFIX = 'features.Cloud.Profiles';

const PROFILES_TABLE_HEADERS = [
  {
    label: ''
  },
  {
    property: 'name',
    label: T.translate(`${PREFIX}.ListView.profileName`)
  },
  {
    property: (profile) => (profile.provisioner.label),
    label: T.translate(`${PREFIX}.common.provisioner`)
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
    label: T.translate(`${PREFIX}.common.last24HrRuns`)
  },
  {
    property: 'last24HrNodeHr',
    label: T.translate(`${PREFIX}.common.last24HrNodeHr`)
  },
  {
    property: 'totalNodeHr',
    label: T.translate(`${PREFIX}.common.totalNodeHr`)
  },
  {
    property: 'schedules',
    label: T.translate(`${PREFIX}.ListView.schedules`)
  },
  {
    property: 'triggers',
    label: T.translate(`${PREFIX}.ListView.triggers`)
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

const NUM_PROFILES_TO_SHOW = 5;

class ProfilesListView extends Component {
  state = {
    profiles: this.props.profiles,
    provisionersMap: {},
    viewAll: false,
    sortMethod: SORT_METHODS.asc,
    sortColumn: PROFILES_TABLE_HEADERS[1].property,
    profileToDelete: null,
    deleteErrMsg: '',
    extendedDeleteErrMsg: ''
  };

  static propTypes = {
    namespace: PropTypes.string.isRequired,
    profiles: PropTypes.array,
    error: PropTypes.any,
    loading: PropTypes.bool
  };

  componentDidMount() {
    getProfiles(this.props.namespace);
    this.getProvisionersMap();
  }

  componentWillReceiveProps(nextProps) {
    if (!isEqual(nextProps.profiles, this.props.profiles)) {
      this.setState({
        profiles: orderBy(nextProps.profiles, this.state.sortColumn, this.state.sortMethod)
      });
    }
  }

  getProvisionersMap() {
    MyCloudApi
      .getProvisioners()
      .subscribe(
        (provisioners) => {
          let provisionersMap = {};
          provisioners.forEach(provisioner => {
            provisionersMap[provisioner.name] = provisioner.label;
          });
          this.setState({
            provisionersMap
          });
        },
        (error) => {
          setError(error.response || error);
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

  deleteProfile = (profile) => {
    let namespace = profile.scope === 'SYSTEM' ? 'system' : this.props.namespace;

    deleteProfile(namespace, profile.name, this.props.namespace)
      .subscribe(() => {
        this.setState({
          profileToDelete: null,
          deleteErrMsg: '',
          extendedDeleteErrMsg: ''
        });
      }, (err) => {
        this.setState({
          deleteErrMsg: T.translate(`${PREFIX}.common.deleteError`),
          extendedDeleteErrMsg: err
        });
      });
  };

  toggleDeleteConfirmationModal = (profileToDelete = null) => {
    this.setState({
      profileToDelete,
      deleteErrMsg: '',
      extendedDeleteErrMsg: ''
    });
  }

  renderProfilesTable() {
    if (!this.state.profiles.length) {
      return (
        <div className="text-xs-center">
          {
            this.props.namespace === 'system' ?
              (
                <span>
                  {T.translate(`${PREFIX}.ListView.noProfilesSystem`)}
                  <Link to={'/ns/system/profiles/create'}>
                    {T.translate(`${PREFIX}.ListView.createOne`)}
                  </Link>
                </span>
              )
            :
              (
                <span>
                  {T.translate(`${PREFIX}.ListView.noProfiles`)}
                  <Link to={`/ns/${getCurrentNamespace()}/profiles/create`}>
                    {T.translate(`${PREFIX}.ListView.createOne`)}
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
          <div />
          <div />
          <div />
          <div />
          <div />
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

    if (!this.state.viewAll && profiles.length > NUM_PROFILES_TO_SHOW) {
      profiles = profiles.slice(0, NUM_PROFILES_TO_SHOW);
    }

    const actionsElem = () => <IconSVG name="icon-cog-empty" />;

    return (
      <div className="grid-body">
        {
          profiles.map((profile) => {
            let namespace = profile.scope === 'SYSTEM' ? 'system' : this.props.namespace;
            let provisionerName = profile.provisioner.name;
            profile.provisioner.label = this.state.provisionersMap[provisionerName] || provisionerName;

            return (
              <Link
                to={`/ns/${namespace}/profiles/details/${profile.name}`}
                className="grid-row grid-link"
                key={uuidV4()}
              >
                <div></div>
                <div title={profile.name}>
                  {profile.name}
                </div>
                <div>{profile.provisioner.label}</div>
                <div>{profile.scope}</div>
                <div />
                <div />
                <div />
                <div />
                <div />
                <div />
                <div />
                <div>
                  <ActionsPopover
                    target={actionsElem}
                    namespace={this.props.namespace}
                    profile={profile}
                    onDeleteClick={this.toggleDeleteConfirmationModal.bind(this, profile)}
                  />
                </div>
              </Link>
            );
          })
        }
      </div>
    );
  }

  renderDeleteConfirmationModal() {
    if (!this.state.profileToDelete) {
      return null;
    }

    const confirmationText = T.translate(`${PREFIX}.common.deleteConfirmation`, {profile: this.state.profileToDelete.name});

    return (
      <ConfirmationModal
        headerTitle={T.translate(`${PREFIX}.common.deleteTitle`)}
        toggleModal={this.toggleDeleteConfirmationModal.bind(this, null)}
        confirmationText={confirmationText}
        confirmButtonText={T.translate('commons.delete')}
        confirmFn={this.deleteProfile.bind(this, this.state.profileToDelete)}
        cancelFn={this.toggleDeleteConfirmationModal.bind(this, null)}
        isOpen={this.state.profileToDelete !== null}
        errorMessage={this.state.deleteErrMsg}
        extendedMessage={this.state.extendedDeleteErrMsg}
      />
    );
  }

  renderError() {
    if (!this.props.error) {
      return null;
    }

    let error = T.translate(`${PREFIX}.ListView.importError`);
    if (typeof this.props.error === 'string') {
      error = `${error}: ${this.props.error}`;
    }

    return (
      <Alert
        message={error}
        type='error'
        showAlert={true}
        onClose={setError.bind(null, null)}
      />
    );
  }

  render() {
    if (this.props.loading) {
      return (
        <div className="text-xs-center">
          <LoadingSVG />
        </div>
      );
    }
    return (
      <div className="profiles-list-view">
        <ViewAllLabel
          arrayToLimit={this.state.profiles}
          limit={NUM_PROFILES_TO_SHOW}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
        {this.renderProfilesTable()}
        <ViewAllLabel
          arrayToLimit={this.state.profiles}
          limit={NUM_PROFILES_TO_SHOW}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
        {this.renderDeleteConfirmationModal()}
        {this.renderError()}
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    profiles: state.profiles,
    loading: state.loading,
    error: state.error
  };
};

const ConnectedProfilesListView = connect(mapStateToProps)(ProfilesListView);

export default function ProfilesListViewFn(props) {
  return (
    <Provider store={ProfilesStore}>
      <ConnectedProfilesListView {...props} />
    </Provider>
  );
}
