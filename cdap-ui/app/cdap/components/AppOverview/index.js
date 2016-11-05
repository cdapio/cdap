/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import classnames from 'classnames';
require('./AppOverview.less');

export default class AppOverview extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: {},
      dimension: null
    };
  }
  componentWillReceiveProps(newProps) {
    if (newProps.isOpen !== this.state.isOpen) {
      this.setState({
        isOpen: newProps.isOpen
      });
    }
  }
  componentDidMount() {
    let width = this.overviewRef.getBoundingClientRect();
    if (width) {
      this.setState({
        dimension: {
          width
        }
      });
    }
  }
  render() {
    let style={};

    if (this.overviewRef) {
      if (this.props.position === 'right') {
        style.left = this.state.dimension.width;
      }
      if (this.props.position === 'left') {
        style.right = this.state.dimension.width;
      }
    }
    return (
      <div
        className={classnames("entity-overview", this.props.position)}
        ref={ref=> this.overviewRef = ref}
      >
        {
          this.state.dimension ?
            <div className="panel panel-default">
              <div className="panel-heading">Panel heading without title</div>
              <div className="panel-body">
                Position: {this.props.position}
                {JSON.stringify(this.props.entity, null, 4)}
              </div>
            </div>
          :
            null
        }
      </div>
    );
  }
}
AppOverview.defaultPropTypes = {
  position: 'left'
};

AppOverview.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string,
    type: PropTypes.string,
    version: PropTypes.string,
    metadata: PropTypes.object, // FIXME: Shouldn't be an object
    icon: PropTypes.string,
    isHydrator: PropTypes.bool
  }),
  position: PropTypes.oneOf([
    'left',
    'right',
    'top',
    'bottom'
  ])
};
