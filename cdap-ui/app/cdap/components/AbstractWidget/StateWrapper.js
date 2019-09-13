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

import React, { PureComponent } from 'react';
import { objectQuery } from 'services/helpers';
import PropTypes from 'prop-types';
import isNil from 'lodash/isNil';

export default class StateWrapper extends PureComponent {
  static propTypes = {
    comp: PropTypes.any,
    value: PropTypes.oneOfType([PropTypes.array, PropTypes.string, PropTypes.number]),
    onChange: PropTypes.func,
    updateAllProperties: PropTypes.func,
    widgetProps: PropTypes.object,
    extraConfig: PropTypes.object,
    disabled: PropTypes.bool,
    errors: PropTypes.object,
  };

  state = {
    value: this.props.value || objectQuery(this.props, 'widgetProps', 'default') || '',
  };

  componentWillReceiveProps(nextProps) {
    if (this.state.value !== nextProps.value) {
      this.setState({ value: nextProps.value });
    }
  }

  onChange = (value) => {
    let v = objectQuery(value, 'target', 'value');
    v = isNil(v) ? value : v;
    this.setState({
      value: v,
    });
    if (typeof objectQuery(this.props, 'onChange') === 'function') {
      this.props.onChange(v);
    }
  };

  render() {
    let { comp: Comp, widgetProps, extraConfig, disabled, errors } = this.props;
    const value = typeof this.state.value === 'function' ? this.state.value() : this.state.value;
    /*
      TL;DR - Get new value for input widget during each render.
      When a function is passed instead of state each re-render gets the upto date value.
      I only envision this to be happening when the AbstractWidget is used inside an
      Accordion.

      Hack Advisory:
        When a AbstractWidget is used inside an Accordion, it clones it once
        and remove the element if its not an active accordion pane. So if I don't want to
        tie the value to a state variable as the value for an input element then I need to
        pass in a function to get a new value everytime the widget is rendered.

        Ways to solve this problem. Use redux or figure out a way to use React context in 16.3+
    */

    return (
      <Comp
        widgetProps={widgetProps}
        value={value}
        onChange={this.onChange}
        updateAllProperties={this.props.updateAllProperties}
        extraConfig={extraConfig}
        disabled={disabled}
        errors={errors}
      />
    );
  }
}
