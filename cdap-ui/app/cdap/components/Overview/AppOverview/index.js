/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, {PropTypes, Component} from 'react';
import isNil from 'lodash/isNil';
import OverviewHeader from 'components/Overview/OverviewHeader';
import OverviewMetaSection from 'components/Overview/OverviewMetaSection';
import AppOverviewTab from 'components/Overview/AppOverview/AppOverviewTab';
import {MyAppApi} from 'api/app';
import NamespaceStore from 'services/NamespaceStore';
import {objectQuery} from 'services/helpers';
import EntityIconMap from 'services/entity-icon-map';
import {MyMetadataApi} from 'api/metadata';
import shortid from 'shortid';
import T from 'i18n-react';
import FastActionToMessage from 'services/fast-action-message-helper';
import capitalize from 'lodash/capitalize';

export default class AppOverview extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity,
      activeTab: '1',
      entityDetail: null,
      loading: false,
      successMessage: null
    };
  }
  componentWillMount() {
    this.fetchAppDetail();
  }
  componentWillReceiveProps(nextProps) {
    let {entity} = nextProps;
    if (!isNil(entity)) {
      this.setState({
        entity,
      }, this.fetchAppDetail.bind(this));
    }
  }
  fetchAppDetail() {
    this.setState({
      loading: true
    });
    let namespace = NamespaceStore.getState().selectedNamespace;
    let entityId = objectQuery(this.props, 'entity', 'id');
    const metadataParams = {
      namespace,
      entityType: 'apps',
      entityId,
      scope: 'SYSTEM'
    };

    if (entityId) {
      MyMetadataApi
        .getProperties(metadataParams)
        .combineLatest(
          MyAppApi.get({
            namespace,
            appId: this.props.entity.id
          })
        )
        .subscribe(
          res => {
          let entityDetail = res[1];
          let properties = res[0];
          let programs = entityDetail.programs.map(prog => {
            prog.uniqueId = shortid.generate();
            return prog;
          });
          let datasets = entityDetail.datasets.map(dataset => {
            dataset.entityId = {
              id: {
                instanceId: dataset.name
              },
              type: 'datasetinstance'
            };
            dataset.uniqueId = shortid.generate();
            return dataset;
          });
          let streams = entityDetail.streams.map(stream => {
            stream.entityId = {
              id: {
                streamName: stream.name
              },
              type: 'stream'
            };
            stream.uniqueId = shortid.generate();
            return stream;
          });
          entityDetail.streams = streams;
          entityDetail.datasets = datasets;
          entityDetail.programs = programs;
          entityDetail.properties = properties;
          entityDetail.id = this.props.entity.id;
          entityDetail.type = 'application';
          this.setState({
            entityDetail
          }, () => {
            setTimeout(() => {
              this.setState({
                loading: false
              });
            }, 1000);
          });
        });
    }
  }
  onFastActionSuccess(action) {
    this.onFastActionUpdate(action);
    if (this.props.onCloseAndRefresh) {
      this.props.onCloseAndRefresh(action);
    }
  }
  onFastActionUpdate(action) {
    let successMessage;
    if (action === 'setPreferences') {
      successMessage = FastActionToMessage(action, {entityType: capitalize(this.props.entity.type)});
    } else {
      successMessage = FastActionToMessage(action);
    }
    this.setState({
      successMessage
    });
  }
  render() {
    if (this.state.loading) {
      return (
        <div className="fa fa-spinner fa-spin fa-3x"></div>
      );
    }
    let artifactName = objectQuery(this.state, 'entityDetail', 'artifact', 'name');
    let icon = EntityIconMap[artifactName] || EntityIconMap['application'];
    let entityType = [
      'cdap-data-pipeline',
      'cdap-data-streams',
      'cdap-batch',
      'cdap-realtime'
    ]
      .indexOf(artifactName) !== -1 ? artifactName : 'application';

    let title = T.translate(`commons.entity.${entityType}.singular`);

    let namespace = NamespaceStore.getState().selectedNamespace;
    return (
      <div className="app-overview">
        <OverviewHeader
          icon={icon}
          title={title}
          linkTo={{
            pathname: `/ns/${namespace}/apps/${this.props.entity.id}`,
            state: {
              entityDetail: this.state.entityDetail,
              entityMetadata: this.props.entity,
              previousPathname: (location.pathname + location.search).replace(/\/cdap\//g, '/')
            }
          }}
          entityType={entityType}
          successMessage={this.state.successMessage}
          onClose={this.props.onClose}
        />
        <OverviewMetaSection
          entity={Object.assign({}, this.state.entityDetail, this.state.entity)}
          onFastActionSuccess={this.onFastActionSuccess.bind(this)}
          onFastActionUpdate={this.onFastActionUpdate.bind(this)}
          showSeparator={true}
        />
        <AppOverviewTab entity={this.state.entityDetail} />
      </div>
    );
  }
}

AppOverview.propTypes = {
  toggleOverview: PropTypes.bool,
  entity: PropTypes.object,
  onClose: PropTypes.func,
  onCloseAndRefresh: PropTypes.func
};
