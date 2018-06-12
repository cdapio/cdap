/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import React, { Component } from 'react';
import OverviewMetaSection from 'components/Overview/OverviewMetaSection';
import ExploreTablesStore from 'services/ExploreTables/ExploreTablesStore';
import {fetchTables} from 'services/ExploreTables/ActionCreator';
import {objectQuery} from 'services/helpers';
import NamespaceStore from 'services/NamespaceStore';
import {MyDatasetApi} from 'api/dataset';
import {MyMetadataApi} from 'api/metadata';
import uuidV4 from 'uuid/v4';
import T from 'i18n-react';
import DatasetDetaildViewTab from 'components/DatasetDetailedView/Tabs';
import FastActionToMessage from 'services/fast-action-message-helper';
import {Redirect} from 'react-router-dom';
import capitalize from 'lodash/capitalize';
import Page404 from 'components/404';
import BreadCrumb from 'components/BreadCrumb';
import PlusButton from 'components/PlusButton';
import Helmet from 'react-helmet';
import queryString from 'query-string';
import {Route, Switch} from 'react-router-dom';
import FieldLevelLineage from 'components/FieldLevelLineage';
require('./DatasetDetailedView.scss');

export default class DatasetDetailedView extends Component {
  constructor(props) {
    super(props);
    let searchObj = queryString.parse(objectQuery(this.props, 'location', 'search'));
    this.state = {
      entityDetail: objectQuery(this.props, 'location', 'state', 'entityDetail') | {
        schema: null,
        programs: []
      },
      loading: true,
      isInvalid: false,
      routeToHome: false,
      successMessage: null,
      notFound: false,
      modalToOpen: objectQuery(searchObj, 'modalToOpen') || '',
      previousPathName: null
    };
  }

  componentWillMount() {
    let selectedNamespace = NamespaceStore.getState().selectedNamespace;
    let {namespace, datasetId} = this.props.match.params;
    let previousPathName = objectQuery(this.props, 'location', 'state', 'previousPathname')  || `/ns/${selectedNamespace}?overviewid=${datasetId}&overviewtype=dataset`;
    if (!namespace) {
      namespace = NamespaceStore.getState().selectedNamespace;
    }
    this.setState({
      previousPathName
    });
    ExploreTablesStore.dispatch(
      fetchTables(namespace)
    );

    this.fetchEntityDetails(namespace, datasetId);
    if (this.state.entityDetail.id) {
      this.setState({
        loading: false
      });
    }

  }

  componentWillReceiveProps(nextProps) {
    let {namespace: currentNamespace, datasetId: currentDatasetId} = this.props.match.params;
    let {namespace: nextNamespace, datasetId: nextDatasetId} = nextProps.match.params;
    if (currentNamespace === nextNamespace && currentDatasetId === nextDatasetId) {
      return;
    }
    let {namespace, datasetId} = nextProps.match.params;
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
      loading: true
    }, () => {
      this.fetchEntityDetails(namespace, datasetId);
    });
  }

  fetchEntityDetails(namespace, datasetId) {
    if (!this.state.entityDetail.schema || this.state.entityDetail.programs.length === 0) {
      const datasetParams = {
        namespace,
        datasetId
      };

      const metadataParams = {
        namespace,
        entityType: 'datasets',
        entityId: datasetId,
        scope: 'SYSTEM'
      };

      MyMetadataApi.getProperties(metadataParams)
        .combineLatest(MyDatasetApi.getPrograms(datasetParams))
        .subscribe(
          (res) => {
            let appId;
            let programs = res[1].map((programObj) => {
              programObj.uniqueId = uuidV4();
              appId = programObj.application;
              programObj.app = appId;
              programObj.name = programObj.program;
              return programObj;
            });

            let entityDetail = {
              programs,
              schema: res[0].schema,
              name: appId, // FIXME: Finalize on entity detail for fast action
              app: appId,
              id: datasetId,
              type: 'dataset',
              properties: res[0]
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
          err => {
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
      successMessage = FastActionToMessage(action, {entityType: capitalize(this.state.entityDetail.type)});
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
        <div className="app-detailed-view dataset-detailed-view loading">
          <div className="fa fa-spinner fa-spin fa-3x"></div>
        </div>
      );
    }

    if (this.state.notFound) {
      return (
        <Page404
          entityType="dataset"
          entityName={this.props.match.params.datasetId}
        />
      );
    }
    let previousPaths = [{
      pathname: this.state.previousPathName,
      label: T.translate('commons.back')
    }];

    let datasetId = this.props.match.params.datasetId;

    return (
      <div className="app-detailed-view dataset-detailed-view">
        <Helmet
          title={T.translate('features.DatasetDetailedView.Title', {datasetId: datasetId})}
        />
        <div className="bread-crumb-wrapper">
          <BreadCrumb
            previousPaths={previousPaths}
            currentStateIcon="icon-datasets"
            currentStateLabel={T.translate('commons.dataset')}
          />
          <PlusButton mode={PlusButton.MODE.resourcecenter} />
        </div>
        <OverviewMetaSection
          entity={this.state.entityDetail}
          onFastActionSuccess={this.goToHome.bind(this)}
          onFastActionUpdate={this.goToHome.bind(this)}
          fastActionToOpen={this.state.modalToOpen}
          showFullCreationTime={true}
        />
        <Switch>
          {/*
            Currently, the field level lineage is a separate page. The intent is to eventually move
            this to a dedicated page that will replace the metadata view.
          */}
          <Route exact path="/ns/:namespace/datasets/:datasetId/fields" render={
            () => {
              return <FieldLevelLineage entityId={datasetId} />;
            }
          } />
          <Route path="/ns/:namespace/datasets/:datasetId/" render={
            () => {
              return (
                <DatasetDetaildViewTab
                  params={this.props.match.params}
                  pathname={this.props.location.pathname}
                  entity={this.state.entityDetail}
                />
              );
            }
          } />
        </Switch>
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

DatasetDetailedView.propTypes = {
  match: PropTypes.object,
  location: PropTypes.object,
};
