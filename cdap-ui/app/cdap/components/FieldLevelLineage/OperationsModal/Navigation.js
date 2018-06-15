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
import {Actions} from 'components/FieldLevelLineage/store/Store';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';

function NavigationView({activeIndex, limit, prev, next}) {
  const prevDisabled = activeIndex === 0;
  const nextDisabled = activeIndex === (limit - 1);

  return (
    <div className="navigation">
      <span
        className={classnames('nav-icon', { 'disabled': prevDisabled })}
        onClick={!prevDisabled && prev}
      >
        <IconSVG name="icon-caret-left" />
      </span>
      <span>{activeIndex + 1}</span>
      <span className="separator">of</span>
      <span>{limit}</span>
      <span
        className={classnames('nav-icon', { 'disabled': nextDisabled })}
        onClick={!nextDisabled && next}
      >
        <IconSVG name="icon-caret-right" />
      </span>
    </div>
  );
}

NavigationView.propTypes = {
  next: PropTypes.func,
  prev: PropTypes.func,
  activeIndex: PropTypes.number,
  limit: PropTypes.number
};

const mapStateToProps = (state) => {
  return {
    activeIndex: state.operations.activeIndex,
    limit: state.operations.backwardOperations.length
  };
};

const mapDispatch = (dispatch) => {
  return {
    next: () => {
      dispatch({
        type: Actions.nextOperation
      });
    },
    prev: () => {
      dispatch({
        type: Actions.prevOperation
      });
    }
  };
};

const Navigation = connect(
  mapStateToProps,
  mapDispatch
)(NavigationView);

export default Navigation;
