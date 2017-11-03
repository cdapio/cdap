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
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import {setGCSLoading} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import {Route, Switch} from 'react-router-dom';
import Page404 from 'components/404';
import {Provider} from 'react-redux';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import GCSPath from 'components/DataPrep/DataPrepBrowser/GCSBrowser/GCSPath';
import ListingInfo from 'components/DataPrep/DataPrepBrowser/GCSBrowser/ListingInfo';
import GCSSearch from 'components/DataPrep/DataPrepBrowser/GCSBrowser/GCSSearch';
import BrowserData from 'components/DataPrep/DataPrepBrowser/GCSBrowser/BrowserData';
import classnames from 'classnames';

require('./GCSBrowser.scss');

const PREFIX = 'features.DataPrep.DataPrepBrowser.GCSBrowser';

export default class GCSBrowser extends Component {
  static propTypes = {
    toggle: PropTypes.func,
    location: PropTypes.object,
    match: PropTypes.object,
    enableRouting: PropTypes.bool,
    onWorkspaceCreate: PropTypes.func
  };

  static defaultProps = {
    enableRouting: true
  };

  onWorkspaceCreate = (file) => {
    const {selectedNamespace: namespace} = NamespaceStore.getState();
    const {connectionId} = DataPrepBrowserStore.getState().gcs;
    setGCSLoading();

    let headers = {
      'Content-Type': file.type
    };

    let params = {
      namespace,
      connectionId,
      activeBucket: file.bucket,
      generation: file.generation,
      blob: file.blob,
      lines: 10000,
      sampler: 'first'
    };

    MyDataPrepApi.readGCSFile(params, null, headers)
      .subscribe(
        res => {
          let {id: workspaceId} = res.values[0];
          if (this.props.enableRouting) {
            window.location.href = `${window.location.origin}/cdap/ns/${namespace}/dataprep/${workspaceId}`;
          }
          if (this.props.onWorkspaceCreate && typeof this.props.onWorkspaceCreate === 'function') {
            this.props.onWorkspaceCreate(workspaceId);
          }
        }
      );
  };

  renderContentBody = () => {
    const BASEPATH = '/ns/:namespace/connections/gcs/:gcsId';
    if (this.props.enableRouting) {
      return (
        <Switch>
          <Route
            path={`${BASEPATH}`}
            render={() => {
              return <BrowserData {...this.props} onWorkspaceCreate={this.onWorkspaceCreate} />;
            }}
          />
          <Route render={Page404} />
        </Switch>
      );
    }
    return <BrowserData {...this.props} onWorkspaceCreate={this.onWorkspaceCreate} />;
  };

  render() {
    return (
      <Provider store={DataPrepBrowserStore}>
        <div className="gcs-browser">
          <div className="top-panel">
            <div className="title">
              <h5>
                <span
                  className="fa fa-fw"
                  onClick={this.props.toggle}
                >
                  <IconSVG name="icon-bars" />
                </span>

                <span>
                  {T.translate(`${PREFIX}.TopPanel.selectData`)}
                </span>
              </h5>
            </div>
          </div>
          <div className={classnames("sub-panel", {'routing-disabled': !this.props.enableRouting})}>
            <div>
              <GCSPath
                baseStatePath={this.props.match.url}
                enableRouting={this.props.enableRouting}
              />
            </div>
            <div>
              <span className="info">
                <ListingInfo />
              </span>
              <div className="search-container">
                <GCSSearch />
              </div>
            </div>
          </div>
          <div className="gcs-content">
            {this.renderContentBody()}
          </div>
        </div>
      </Provider>
    );
  }
}
