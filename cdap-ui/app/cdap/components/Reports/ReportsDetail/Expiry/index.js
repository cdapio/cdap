/*
 * Copyright © 2018 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import Duration from 'components/Duration';

require('./Expiry.scss');

function ExpiryView({expiry}) {
  if (!expiry) {
    return (
      <span className="expiry-saved">
        Saved
      </span>
    );
  }

  return (
    <span className="expiry">
      <span>Expires in</span>

      <Duration
        targetTime={expiry}
        isMillisecond={false}
      />
    </span>
  );
}

ExpiryView.propTypes = {
  expiry: PropTypes.number
};

const mapStateToProps = (state) => {
  return {
    expiry: state.details.expiry
  };
};

const Expiry = connect(
  mapStateToProps
)(ExpiryView);

export default Expiry;
