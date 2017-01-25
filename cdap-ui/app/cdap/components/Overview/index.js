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

import React, {PropTypes, Component} from 'react';
import AppOverview from 'components/Overview/AppOverview';
import {objectQuery} from 'services/helpers';
import isNil from 'lodash/isNil';
import classnames from 'classnames';
require('./Overview.scss');

export default class Overview extends Component {
  constructor(props) {
    super(props);
    this.state = {
      toggleOverview: this.props.toggleOverview,
      entity: this.props.entity,
      tag: null
    };
    this.typeToComponentMap = {
      'application': AppOverview
    };
  }
  componentWillReceiveProps(nextProps) {
    let {toggleOverview, entity } = nextProps;
    let hasEntityChanged = !isNil(entity) && objectQuery(this.props.entity, 'id') !== objectQuery(entity, 'id');
    if (
      this.props.toggleOverview !== toggleOverview ||
      hasEntityChanged
    ) {
      let tag = this.typeToComponentMap[objectQuery(entity, 'type')];
      this.setState({
        toggleOverview,
        entity,
        tag
      });
    }
  }
  hideOverview() {
    this.setState({
      toggleOverview: false,
      entity: null
    });
    if (this.props.onClose) {
      this.props.onClose();
    }
  }
  render() {
    let Tag = this.state.tag || 'div';
    return (
      <div className={classnames("overview-container", {"show-overview": this.state.toggleOverview })}>
        <div className="overview-wrapper" >
          {
            React.createElement(
              Tag,
              {
                entity: this.state.entity,
                onClose: this.hideOverview.bind(this)
              }
            )
          }
        </div>
      </div>
    );
  }
}

Overview.propTypes = {
  toggleOverview: PropTypes.bool,
  entity: PropTypes.object,
  onClose: PropTypes.func
};
