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
import FunctionDropdownRow, {
  IDropdownOption,
} from 'components/AbstractWidget/FunctionDropdownAliasWidget/FuctionDropdownRow';
import ThemeWrapper from 'components/ThemeWrapper';
import AbstractMultiRowWidget, {
  IMultiRowProps,
} from 'components/AbstractWidget/AbstractMultiRowWidget';

interface IFunctionDropdownWidgetProps extends IMultiRowProps {
  placeholders?: Record<string, string>;
  dropdownOptions: IDropdownOption[];
}

class FunctionDropdownWidget extends AbstractMultiRowWidget<IFunctionDropdownWidgetProps> {
  public renderRow = (id, index) => {
    return (
      <FunctionDropdownRow
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
        dropdownOptions={this.props.dropdownOptions}
        forwardedRef={this.values[id].ref}
      />
    );
  };
}

export default function StyledFunctionDropdownWrapper(props) {
  return (
    <ThemeWrapper>
      <FunctionDropdownWidget {...props} />
    </ThemeWrapper>
  );
}

(StyledFunctionDropdownWrapper as any).propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func,
  disabled: PropTypes.bool,
  placeholders: PropTypes.shape({
    field: PropTypes.string,
    alias: PropTypes.string,
  }),
  dropdownOptions: PropTypes.array,
};
