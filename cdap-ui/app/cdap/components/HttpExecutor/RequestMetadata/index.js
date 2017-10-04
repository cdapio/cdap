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

import PropTypes from 'prop-types';

import React from 'react';
import Body from 'components/HttpExecutor/RequestMetadata/Body';
import Header from 'components/HttpExecutor/RequestMetadata/Header';
import {connect} from 'react-redux';
import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import classnames from 'classnames';
import T from 'i18n-react';

const PREFIX = 'features.HttpExecutor';

const mapStateToProps = (state) => {
  return {
    activeTab: state.http.activeTab,
    method: state.http.method
  };
};

const mapDispatch = (dispatch) => {
  return {
    onTabClick: (tab, isDisabled) => {
      if (isDisabled) { return; }

      dispatch({
        type: HttpExecutorActions.setRequestTab,
        payload: {
          activeTab: tab
        }
      });
    }
  };
};

function RequestMetadataView({activeTab, onTabClick, method}) {
  let bodyDisabled = ['GET', 'DELETE'].indexOf(method) !== -1;

  return (
    <div className="request-metadata">
      <div className="metadata-header-row">
        <div
          className={classnames('metadata-header', {
            active: activeTab === 0,
          })}
          onClick={onTabClick.bind(null, 0, false)}
        >
          <span>{T.translate(`${PREFIX}.header`)}</span>
        </div>
        <div
          className={classnames('metadata-header', {
            active: activeTab === 1,
            disabled: bodyDisabled
          })}
          onClick={onTabClick.bind(null, 1, bodyDisabled)}
        >
          <span>{T.translate(`${PREFIX}.body`)}</span>
        </div>
      </div>

      <div className="metadata-body-row">
        <MetadataRequestBody activeTab={activeTab} />
      </div>
    </div>
  );
}

RequestMetadataView.propTypes = {
  activeTab: PropTypes.number,
  onTabClick: PropTypes.func,
  method: PropTypes.string
};

function MetadataRequestBody({activeTab}) {
  switch (activeTab) {
    case 0:
      return <Header />;
    case 1:
      return <Body />;
    default:
      return null;
  }
}

MetadataRequestBody.propTypes = {
  activeTab: PropTypes.number
};

const RequestMetadata = connect(
  mapStateToProps,
  mapDispatch
)(RequestMetadataView);

export default RequestMetadata;
