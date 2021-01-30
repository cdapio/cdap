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
import T from 'i18n-react';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import {
  setS3Loading,
  setError,
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import BucketDataView from 'components/DataPrep/DataPrepBrowser/S3Browser/BucketData';
import { Route, Switch } from 'react-router-dom';
import { Provider } from 'react-redux';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import S3Path from 'components/DataPrep/DataPrepBrowser/S3Browser/S3Path';
import ListingInfo from 'components/DataPrep/DataPrepBrowser/S3Browser/ListingInfo';
import S3Search from 'components/DataPrep/DataPrepBrowser/S3Browser/S3Search';
import classnames from 'classnames';
import DataPrepBrowserPageTitle from 'components/DataPrep/DataPrepBrowser/PageTitle';
import DataprepBrowserTopPanel from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserTopPanel';
import history from 'services/history';

require('./S3Browser.scss');

const PREFIX = 'features.DataPrep.DataPrepBrowser.S3Browser';
export default class S3Browser extends Component {
  static propTypes = {
    toggle: PropTypes.func,
    match: PropTypes.object,
    enableRouting: PropTypes.bool,
    onWorkspaceCreate: PropTypes.func,
    scope: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
    showPanelToggle: PropTypes.bool,
  };

  static defaultProps = {
    enableRouting: true,
  };

  onWorkspaceCreate = (file) => {
    const { selectedNamespace: namespace } = NamespaceStore.getState();
    const { connectionId, prefix } = DataPrepBrowserStore.getState().s3;
    let activeBucket = prefix;
    /**
     * This is to extract the bucket name from S3 for url param
     * eg: /bucket-1/folder1/folder2/file.csv
     * we need /bucket-1 alone for the api request
     */
    if (activeBucket.slice(1).split('/').length > 1) {
      activeBucket = activeBucket.slice(0, activeBucket.slice(1).indexOf('/') + 1);
    }
    setS3Loading();
    let headers = {
      'Content-Type': file.type,
    };
    let params = {
      context: namespace,
      connectionId,
      activeBucket,
      key: file.path,
      lines: 10000,
      sampler: 'first',
    };
    if (this.props.scope) {
      params.scope = this.props.scope;
    }
    MyDataPrepApi.readS3File(params, null, headers).subscribe(
      (res) => {
        let { id: workspaceId } = res.values[0];
        if (this.props.enableRouting) {
          history.push(`/ns/${namespace}/wrangler/${workspaceId}`);
        }
        if (this.props.onWorkspaceCreate && typeof this.props.onWorkspaceCreate === 'function') {
          this.props.onWorkspaceCreate(workspaceId);
        }
      },
      (err) => {
        setError(err);
      }
    );
  };

  renderContentBody = () => {
    const BASEPATH = '/ns/:namespace/connections/s3/:s3Id';
    if (this.props.enableRouting) {
      return (
        <Switch>
          <Route
            path={`${BASEPATH}`}
            render={() => {
              return <BucketDataView {...this.props} onWorkspaceCreate={this.onWorkspaceCreate} />;
            }}
          />
        </Switch>
      );
    }
    return <BucketDataView {...this.props} onWorkspaceCreate={this.onWorkspaceCreate} />;
  };

  render() {
    return (
      <Provider store={DataPrepBrowserStore}>
        <div className="s3-browser">
          {this.props.enableRouting ? (
            <DataPrepBrowserPageTitle
              browserI18NName="S3Browser"
              browserStateName="s3"
              locationToPathInState={['prefix']}
            />
          ) : null}
          <DataprepBrowserTopPanel
            allowSidePanelToggle={true}
            toggle={this.props.toggle}
            browserTitle={T.translate(`${PREFIX}.TopPanel.selectData`)}
            showPanelToggle={this.props.showPanelToggle}
          />
          <div
            className={classnames('sub-panel', { 'routing-disabled': !this.props.enableRouting })}
          >
            <div className="path-container">
              <S3Path
                baseStatePath={this.props.enableRouting ? this.props.match.url : '/'}
                enableRouting={this.props.enableRouting}
              />
            </div>
            <div className="info-container">
              <span className="info">
                <ListingInfo />
              </span>
              <div className="search-container">
                <S3Search />
              </div>
            </div>
          </div>
          <div className="s3-content">{this.renderContentBody()}</div>
        </div>
      </Provider>
    );
  }
}
