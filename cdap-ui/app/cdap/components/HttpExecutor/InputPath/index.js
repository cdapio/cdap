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

import React, { PropTypes } from 'react';
import {connect} from 'react-redux';
import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import T from 'i18n-react';

const PREFIX = 'features.HttpExecutor';

const mapStateToProps = (state) => {
  return {
    value: state.http.path
  };
};

const mapDispatch = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: HttpExecutorActions.setPath,
        payload: {
          path: e.target.value
        }
      });
    }
  };
};

function InputPathView({value, onChange}) {
  let url = [
    window.CDAP_CONFIG.sslEnabled ? 'https://' : 'http://',
    window.CDAP_CONFIG.cdap.routerServerUrl,
    ':',
    window.CDAP_CONFIG.sslEnabled ? window.CDAP_CONFIG.cdap.routerSSLServerPort : window.CDAP_CONFIG.cdap.routerServerPort,
    '/v3/'
  ].join('');

  return (
    <div className="input-path-container">
      <div className="input-group">
        <span className="input-group-addon">
          {url}
        </span>
        <input
          type="text"
          className="form-control"
          placeholder={T.translate(`${PREFIX}.path`)}
          value={value}
          onChange={onChange}
        />
      </div>
    </div>
  );
}

InputPathView.propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func
};

const InputPath = connect(
  mapStateToProps,
  mapDispatch
)(InputPathView);

export default InputPath;
