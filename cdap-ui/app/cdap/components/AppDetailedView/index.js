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

import React, {Component, PropTypes} from 'react';
import Helmet from 'react-helmet';
import classnames from 'classnames';
import T from 'i18n-react';
import {objectQuery} from 'services/helpers';
import {MyAppApi} from 'api/app';
import shortid from 'shortid';
import Router from 'react-router/BrowserRouter';
import Miss from 'react-router/Miss';
import Page404 from 'components/404';
import {getIcon} from 'services/helpers';
import AppDetailedViewTabConfig from './AppDetailedViewTabConfig';
import ConfigurableTab from 'components/ConfigurableTab';
import ApplicationMetrics from 'components/EntityCard/ApplicationMetrics';

require('./AppDetailedView.scss');
import ExploreTablesStore from 'services/ExploreTables/ExploreTablesStore';
import {fetchTables} from 'services/ExploreTables/ActionCreator';

export default class AppDetailedView extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entityDetail: objectQuery(this.props, 'location', 'state', 'entityDetail') || {
        programs: [],
        datasets: [],
        streams: []
      },
      entityMetata: objectQuery(this.props, 'location', 'state', 'entityMetadata') || {},
      isInvalid: false
    };
  }
  getChildContext() {
    return {
      entity: this.state.entityDetail
    };
  }
  componentWillMount() {
    let {namespace, appId} = this.props.params;
    ExploreTablesStore.dispatch(
     fetchTables(namespace)
   );
    if (this.state.entityDetail.programs.length === 0) {
      MyAppApi
        .get({
          namespace,
          appId
        })
        .map(entityDetail => {
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
          return entityDetail;
        })
        .subscribe(
          entityDetail => {
            this.setState({ entityDetail });
          },
          (err) => {
            if (err.statusCode === 404) {
              this.setState({isInvalid: true});
            }
          }
        );
    }
  }
  render() {
    return (
      <Router>
        <div>
          <Helmet
            title={T.translate('features.AppDetailedView.Title', {appId: this.props.params.appId})}
          />
          <div className={
              classnames(
                "app-detailed-view",
                this.state.entityDetail.configuration ?
                  'datapipeline'
                :
                  'application'
              )}>
            <div className="app-detailed-view-header">
              <i className={`fa ${getIcon('application')}`}></i>
              <span>
                {this.state.entityDetail.name}
                <small>1.0.0</small>
              </span>
              <span className="text-xs-right hidden">
                <i className="fa fa-info fa-lg"></i>
              </span>
            </div>
            {
              this.state.entityDetail.description ?
                <div className="app-detailed-view-description">
                  {this.state.entityDetail.description}
                </div>
              :
                null
            }
            <div className="app-detailed-view-content">
              {
                <ApplicationMetrics entity={{
                  id: this.props.params.appId
                }}/>
              }
              <ConfigurableTab
                tabConfig={AppDetailedViewTabConfig}
              />
            </div>
          </div>
          {
            this.state.isInvalid ?
              <Miss component={Page404} />
            :
              null
          }
        </div>
      </Router>
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
const entityMetadataType = PropTypes.shape({
  id: PropTypes.string,
  type: PropTypes.string,
  version: PropTypes.string,
  metadata: PropTypes.object, // FIXME: Shouldn't be an object
  icon: PropTypes.string,
  isHydrator: PropTypes.bool
});
AppDetailedView.childContextTypes = {
  entity: PropTypes.object
};

AppDetailedView.propTypes = {
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
      entityMetadata: entityMetadataType
    })
  })
};
