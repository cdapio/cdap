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
import PropTypes from 'prop-types';
import AbstractWidgetFactory from 'components/AbstractWidget/AbstractWidgetFactory';
import StateWrapper from 'components/AbstractWidget/StateWrapper';

export const WIDGET_PROPTYPES = {
  widgetProps: PropTypes.object,
  value: PropTypes.oneOfType([
    PropTypes.array,
    PropTypes.string,
    PropTypes.number
  ]),
  onChange: PropTypes.func
};
export const DEFAULT_WIDGET_PROPS = {
  widgetProps: {},
  value: '',
  onChange: () => {}
};
export default class AbstractWidget extends Component {
  static propTypes = {
    type: PropTypes.oneOf(Object.keys(AbstractWidgetFactory)),
    ...WIDGET_PROPTYPES
  };

  render() {
    let Comp = AbstractWidgetFactory[this.props.type];
    return (
      <StateWrapper
        comp={Comp}
        onChange={this.props.onChange}
        value={this.props.value}
        widgetProps={this.props.widgetProps}
      />
    );
  }
}
