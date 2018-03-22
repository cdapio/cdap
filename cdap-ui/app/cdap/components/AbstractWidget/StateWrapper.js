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

import React, {Component} from 'react';
import {objectQuery} from 'services/helpers';
import PropTypes from 'prop-types';
import isNil from 'lodash/isNil';

export default class StateWrapper extends Component {
  static propTypes = {
    comp: PropTypes.any,
    value: PropTypes.oneOfType([PropTypes.array, PropTypes.string, PropTypes.number]),
    onChange: PropTypes.func,
    widgetProps: PropTypes.object
  };

  state = {
    value: this.props.value || objectQuery(this.props, 'widgetProps', 'default')
  };

  onChange = (value) => {
    let v = objectQuery(value, 'target', 'value');
    v = isNil(v) ? value : v;
    this.setState({
      value: v
    });
    if (typeof objectQuery(this.props, 'onChange') === 'function') {
      this.props.onChange(v);
    }
  };

  render() {
    let {comp: Comp, widgetProps} = this.props;
    return (
      <Comp
        widgetProps={widgetProps}
        value={this.state.value}
        onChange={this.onChange}
      />
    );
  }
}
