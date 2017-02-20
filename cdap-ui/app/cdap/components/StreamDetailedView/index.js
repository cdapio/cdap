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

import React, { Component, PropTypes } from 'react';
import OverviewMetaSection from 'components/Overview/OverviewMetaSection';
import ExploreTablesStore from 'services/ExploreTables/ExploreTablesStore';
import {fetchTables} from 'services/ExploreTables/ActionCreator';
import {objectQuery} from 'services/helpers';
import NamespaceStore from 'services/NamespaceStore';
import {MyStreamApi} from 'api/stream';
import {MyMetadataApi} from 'api/metadata';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import shortid from 'shortid';
import T from 'i18n-react';
import StreamDetaildViewTab from 'components/StreamDetailedView/Tabs';
import {MySearchApi} from 'api/search';
import {parseMetadata} from 'services/metadata-parser';
import FastActionToMessage from 'services/fast-action-message-helper';
import capitalize from 'lodash/capitalize';
import Redirect from 'react-router/Redirect';
import Page404 from 'components/404';
import BreadCrumb from 'components/BreadCrumb';
import ResourceCenterButton from 'components/ResourceCenterButton';
import Helmet from 'react-helmet';

require('./StreamDetailedView.scss');

export default class StreamDetailedView extends Component {
  constructor(props) {
    super(props);

    this.state = {
      entityDetail: objectQuery(this.props, 'location', 'state', 'entityDetail') | {
        schema: null,
        programs: []
      },
      loading: true,
      entityMetadata: objectQuery(this.props, 'location', 'state', 'entityMetadata') || {},
      isInvalid: false,
      routeToHome: false,
      successMessage: null,
      notFound: false,
      modalToOpen: objectQuery(this.props, 'location', 'query', 'modalToOpen') || ''
    };
  }

  componentWillMount() {
    let {namespace, streamId} = this.props.params;
    if (!namespace) {
      namespace = NamespaceStore.getState().selectedNamespace;
    }
    ExploreTablesStore.dispatch(
      fetchTables(namespace)
    );

    this.fetchEntityDetails(namespace, streamId);
    this.fetchEntityMetadata(namespace, streamId);
    if (
      isNil(this.state.entityMetadata) ||
      isEmpty(this.state.entityMetadata) ||
      isNil(this.state.entity) ||
      isEmpty(this.state.entity)
    ) {
      this.setState({
        loading: true
      });
    }

  }

  componentWillReceiveProps(nextProps) {
    let {namespace: currentNamespace, streamId: currentStreamId} = this.props.params;
    let {namespace: nextNamespace, streamId: nextStreamId} = nextProps.params;
    if (currentNamespace === nextNamespace && currentStreamId === nextStreamId) {
      return;
    }
    let {namespace, streamId} = nextProps.params;
    if (!namespace) {
      namespace = NamespaceStore.getState().selectedNamespace;
    }
    ExploreTablesStore.dispatch(
      fetchTables(namespace)
    );

    this.setState({
      entityDetail: {
        schema: null,
        programs: []
      },
      loading: true,
      entityMetadata: {}
    }, () => {
      this.fetchEntityDetails(namespace, streamId);
      this.fetchEntityMetadata(namespace, streamId);
    });
  }

  fetchEntityDetails(namespace, streamId) {
    if (!this.state.entityDetail.schema || this.state.entityDetail.programs.length === 0) {
      const streamParams = {
        namespace,
        streamId
      };

      const metadataParams = {
        namespace,
        entityType: 'streams',
        entityId: streamId,
        scope: 'SYSTEM'
      };

      MyMetadataApi.getProperties(metadataParams)
        .combineLatest(MyStreamApi.getPrograms(streamParams))
        .subscribe(
          (res) => {
            let appId;
            let programs = res[1].map((program) => {
              program.uniqueId = shortid.generate();
              appId = program.application.applicationId;
              program.app = appId;
              program.name = program.id;
              return program;
            });

            let entityDetail = {
              programs,
              schema: res[0].schema,
              name: appId, // FIXME: Finalize on entity detail for fast action
              app: appId,
              id: streamId,
              type: 'stream'
            };

            this.setState({
              entityDetail
            }, () => {
              setTimeout(() => {
                this.setState({
                  loading: false
                });
              }, 1000);
            });
          },
          (err) => {
            if (err.statusCode === 404) {
              this.setState({
                notFound: true,
                loading: false
              });
            }
          }
        );
    }
  }

  fetchEntityMetadata(namespace) {
    if (
      isNil(this.state.entityMetadata) ||
      isEmpty(this.state.entityMetadata)
    ) {
      // FIXME: This is NOT the right way. Need to figure out a way to be more efficient and correct.

      MySearchApi
        .search({
          namespace,
          query: this.props.params.streamId
        })
        .map(res => res.results.map(parseMetadata))
        .subscribe(entityMetadata => {
          if (!entityMetadata.length) {
            this.setState({
              loading: false,
              notFound: true
            });
          } else {
            let metadata = entityMetadata
              .filter(en => en.type === 'stream')
              .find( en => en.id === this.props.params.streamId);
            this.setState({
              entityMetadata: metadata,
              loading: false
            });
          }
        });
    }
  }

  goToHome(action) {
    if (action === 'delete') {
      let selectedNamespace = NamespaceStore.getState().selectedNamespace;
      this.setState({
        selectedNamespace,
        routeToHome: true
      });
    }
    let successMessage;
    if (action === 'setPreferences') {
      successMessage = FastActionToMessage(action, {entityType: capitalize(this.state.entityMetadata.type)});
    } else {
      successMessage = FastActionToMessage(action);
    }
    this.setState({
      successMessage
    }, () => {
      setTimeout(() => {
        this.setState({
          successMessage: null
        });
      }, 3000);
    });
  }

  render() {
    if (this.state.loading) {
      return (
        <div className="app-detailed-view">
          <div className="fa fa-spinner fa-spin fa-3x"></div>
        </div>
      );
    }

    if (this.state.notFound) {
      return (
        <Page404
          entityType="Stream"
          entityName={this.props.params.streamId}
        />
      );
    }

    let selectedNamespace = NamespaceStore.getState().selectedNamespace;
    let previousPathname = objectQuery(this.props, 'location', 'state', 'previousPathname')  || `/ns/${selectedNamespace}`;
    let previousPaths = [{
      pathname: previousPathname,
      label: T.translate('commons.back')
    }];
    return (
      <div className="app-detailed-view streams-deatiled-view">
        <Helmet
          title={T.translate('features.StreamDetailedView.Title', {streamId: this.props.params.streamId})}
        />
        <ResourceCenterButton />
        <BreadCrumb
          previousPaths={previousPaths}
          currentStateIcon="icon-streams"
          currentStateLabel={T.translate('commons.stream')}
        />
        <OverviewMetaSection
          entity={this.state.entityMetadata}
          onFastActionSuccess={this.goToHome.bind(this)}
          onFastActionUpdate={this.goToHome.bind(this)}
          fastActionToOpen={this.state.modalToOpen}
          showFullCreationTime={true}
        />
        <StreamDetaildViewTab
          params={this.props.params}
          pathname={this.props.location.pathname}
          entity={this.state.entityDetail}
        />
        {
          this.state.routeToHome ?
            <Redirect to={`/ns/${this.state.selectedNamespace}`} />
          :
            null
        }
      </div>
    );
  }
}

StreamDetailedView.propTypes = {
  params: PropTypes.shape({
    streamId: PropTypes.string,
    namespace: PropTypes.string
  }),
  location: PropTypes.any
};
