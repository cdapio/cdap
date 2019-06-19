/*
 * Copyright © 2019 Cask Data, Inc.
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

import * as React from 'react';
import PropTypes from 'prop-types';
import MultipleValuesRow from 'components/AbstractWidget/MultipleValuesWidget/MultipleValuesRow';
import ThemeWrapper from 'components/ThemeWrapper';
import AbstractMultiRowWidget, {
  IMultiRowProps,
} from 'components/AbstractWidget/AbstractMultiRowWidget';

interface IMultipleValuesWidgetProps extends IMultiRowProps {
  placeholders?: string[];
  valuesDelimiter?: string;
  numValues: string | number;
}

class MultipleValuesWidget extends AbstractMultiRowWidget<IMultipleValuesWidgetProps> {
  public renderRow = (id, index) => {
    let numValues = this.props.numValues;
    if (typeof numValues === 'string') {
      numValues = parseInt(numValues, 10);
      numValues = isNaN(numValues) ? null : numValues;
    }

    return (
      <MultipleValuesRow
        key={id}
        value={this.values[id].value}
        id={id}
        index={index}
        onChange={this.editRow}
        addRow={this.addRow.bind(this, index)}
        removeRow={this.removeRow.bind(this, index)}
        autofocus={this.state.autofocus === id}
        changeFocus={this.changeFocus}
        disabled={this.props.disabled}
        placeholders={this.props.placeholders}
        valuesDelimiter={this.props.valuesDelimiter}
        numValues={numValues}
        forwardedRef={this.values[id].ref}
      />
    );
  };
}

export default function StyledMultipleValuesWidgetWrapper(props) {
  return (
    <ThemeWrapper>
      <MultipleValuesWidget {...props} />
    </ThemeWrapper>
  );
}

(StyledMultipleValuesWidgetWrapper as any).propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func,
  disabled: PropTypes.bool,
  delimiter: PropTypes.string,
  placeholders: PropTypes.arrayOf(PropTypes.string),
  valuesDelimiter: PropTypes.string,
  numValues: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
};
