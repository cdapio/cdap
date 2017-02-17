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

import React, {PropTypes, Component} from 'react';
import {objectQuery} from 'services/helpers';
import {MyAppApi} from 'api/app';
import ExploreTablesStore from 'services/ExploreTables/ExploreTablesStore';
import {fetchTables} from 'services/ExploreTables/ActionCreator';
import OverviewMetaSection from 'components/Overview/OverviewMetaSection';
import T from 'i18n-react';
import {MySearchApi} from 'api/search';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import NamespaceStore from 'services/NamespaceStore';
import BreadCrumb from 'components/BreadCrumb';
import {parseMetadata} from 'services/metadata-parser';
import AppDetailedViewTab from 'components/AppDetailedView/Tabs';
import shortid from 'shortid';
import Redirect from 'react-router/Redirect';
import FastActionToMessage from 'services/fast-action-message-helper';
import capitalize from 'lodash/capitalize';
import Page404 from 'components/404';
import ResourceCenterButton from 'components/ResourceCenterButton';
import Helmet from 'react-helmet';
require('./AppDetailedView.scss');

export default class AppDetailedView extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entityDetail: objectQuery(this.props, 'location', 'state', 'entityDetail') || {
        programs: [],
        datasets: [],
        streams: [],
        routeToHome: false,
        selectedNamespace: null,
        successMessage: null,
        notFound: false
      },
      loading: true,
      entityMetadata: objectQuery(this.props, 'location', 'state', 'entityMetadata') || {},
      isInvalid: false
    };
  }
  componentWillMount() {
    let {namespace, appId} = this.props.params;
    if (!namespace) {
      namespace = NamespaceStore.getState().selectedNamespace;
    }
    ExploreTablesStore.dispatch(
      fetchTables(namespace)
    );

    if (this.state.entityDetail.programs.length === 0) {
      MyAppApi
        .get({
          namespace,
          appId
        })
        .subscribe(
          entityDetail => {
            if (isEmpty(entityDetail)) {
              this.setState({
                notFound: true,
                loading: false
              });
            }
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
            this.setState({
              entityDetail
            });
          },
          err => {
            if (err.statusCode === 404) {
              this.setState({
                notFound: true
              });
            }
          }
      );
    }
    if (
      isNil(this.state.entityMetadata) ||
      isEmpty(this.state.entityMetadata)
    ) {
      // FIXME: This is NOT the right way. Need to figure out a way to be more efficient and correct.
      MySearchApi
        .search({
          namespace,
          query: this.props.params.appId
        })
        .map(res => res.results.map(parseMetadata))
        .subscribe(entityMetadata => {
          if (!entityMetadata.length) {
            this.setState({
              notFound: true,
              loading: false
            });
          }
          this.setState({
            entityMetadata: entityMetadata[0],
            loading: false
          });
        });
    }

    if (
      !isNil(this.state.entityMetadata) &&
      !isEmpty(this.state.entityMetadata) &&
      !isNil(this.state.entityDetail) &&
      !isEmpty(this.state.entityDetail)
    ) {
      this.setState({
        loading: false
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
    if (this.state.notFound) {
      return (
        <Page404
          entityType="Application"
          entityName={this.props.params.appId}
        />
      );
    }
    if (this.state.loading) {
      return (
        <div className="app-detailed-view">
          <div className="fa fa-spinner fa-spin fa-3x"></div>
        </div>
      );
    }
    let selectedNamespace = NamespaceStore.getState().selectedNamespace;
    let previousPathname = objectQuery(this.props, 'location', 'state', 'previousPathname')  || `/ns/${selectedNamespace}`;
    let previousPaths = [{
      pathname: previousPathname,
      label: T.translate('commons.back')
    }];
    return (
      <div className="app-detailed-view">
        <Helmet
          title={T.translate('features.AppDetailedView.Title', {appId: this.props.params.appId})}
        />
        <ResourceCenterButton />
        <BreadCrumb
          previousPaths={previousPaths}
          currentStateIcon="icon-fist"
          currentStateLabel={T.translate('commons.application')}
        />
        <OverviewMetaSection
          entity={this.state.entityMetadata}
          onFastActionSuccess={this.goToHome.bind(this)}
          onFastActionUpdate={this.goToHome.bind(this)}
          showFullCreationTime={true}
        />
        <AppDetailedViewTab
          params={this.props.params}
          pathname={this.props.location.pathname}
          entity={this.state.entityDetail}/
        >
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

const entityDetailType = PropTypes.shape({
  artifact: PropTypes.shape({
    name: PropTypes.string,
    scope: PropTypes.string,
    version: PropTypes.string
  }),
  artifactVersion: PropTypes.string,
  configuration: PropTypes.string,
  // Need to expand on these
  datasets: PropTypes.arrayOf(PropTypes.object),
  streams: PropTypes.arrayOf(PropTypes.object),
  plugins: PropTypes.arrayOf(PropTypes.object),
  programs: PropTypes.arrayOf(PropTypes.object),
});


AppDetailedView.propTypes = {
  entity: PropTypes.object,
  params: PropTypes.shape({
    appId: PropTypes.string,
    namespace: PropTypes.string
  }),
  location: PropTypes.shape({
    hash: PropTypes.string,
    pathname: PropTypes.string,
    query: PropTypes.any,
    search: PropTypes.string,
    state: PropTypes.shape({
      entityDetail: entityDetailType,
      entityMetadata: PropTypes.object,
      previousPathname: PropTypes.string
    })
  })
};
