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
import {connect} from 'react-redux';
import classnames from 'classnames';
import T from 'i18n-react';

const PREFIX = 'features.HttpExecutor';

const mapStateToProps = (state) => {
  return {
    code: state.http.statusCode
  };
};

function StatusCodeView({code}) {
  return (
    <div className="status-code">
      <div className="status-title">
        {T.translate(`${PREFIX}.statusCode`)}
      </div>

      <div className={classnames('code', {
        'text-success': code < 300,
        'text-danger': code !== null && code >= 300
      })}>
        {code}
      </div>
    </div>
  );
}

StatusCodeView.propTypes = {
  code: PropTypes.number
};

const StatusCode = connect(
  mapStateToProps
)(StatusCodeView);

export default StatusCode;
