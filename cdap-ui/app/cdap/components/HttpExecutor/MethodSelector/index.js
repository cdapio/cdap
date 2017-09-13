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


const mapStateToProps = (state) => {
  return {
    value: state.http.method
  };
};

const mapDispatch = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: HttpExecutorActions.setMethod,
        payload: {
          method: e.target.value
        }
      });
    }
  };
};

function MethodSelectorView({onChange, value}) {
  const METHODS = [
    'DELETE',
    'GET',
    'POST',
    'PUT',
  ];

  return (
    <div className="method-selector-container">
      <select
        className="form-control"
        value={value}
        onChange={onChange}
      >
        {
          METHODS.map((method) => {
            return (
              <option
                value={method}
                key={method}
              >
                {method}
              </option>
            );
          })
        }
      </select>
    </div>
  );
}

MethodSelectorView.propTypes = {
  value: PropTypes.oneOf(['GET', 'POST', 'PUT', 'DELETE']),
  onChange: PropTypes.func
};

const MethodSelector = connect(
  mapStateToProps,
  mapDispatch
)(MethodSelectorView);

export default MethodSelector;
