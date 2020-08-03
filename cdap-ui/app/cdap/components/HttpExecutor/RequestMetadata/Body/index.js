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

import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import PropTypes from 'prop-types';
import React from 'react';
import { connect } from 'react-redux';

const mapStateToProps = (state) => {
  return {
    value: state.http.body,
  };
};

const mapDispatch = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: HttpExecutorActions.setBody,
        payload: {
          body: e.target.value,
        },
      });
    },
  };
};

function BodyView({ value, onChange }) {
  return (
    <div className="request-body">
      <textarea data-cy="request-body" className="form-control" value={value} onChange={onChange} />
    </div>
  );
}

BodyView.propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func,
};

const Body = connect(mapStateToProps, mapDispatch)(BodyView);

export default Body;
