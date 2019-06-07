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
import KeyValueDropdownRow, {
  IDropdownOption,
} from 'components/AbstractWidget/KeyValueDropdownWidget/KeyValueDropdownRow';
import ThemeWrapper from 'components/ThemeWrapper';
import AbstractMultiRowWidget, {
  IMultiRowProps,
} from 'components/AbstractWidget/AbstractMultiRowWidget';

interface IKeyValueDropdownWidgetProps extends IMultiRowProps {
  keyPlaceholder?: string;
  kvDelimiter?: string;
  dropdownOptions: IDropdownOption[];
}

class KeyValueDropdownWidget extends AbstractMultiRowWidget<IKeyValueDropdownWidgetProps> {
  public renderRow = (id, index) => {
    return (
      <KeyValueDropdownRow
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
        keyPlaceholder={this.props.keyPlaceholder}
        kvDelimiter={this.props.kvDelimiter}
        dropdownOptions={this.props.dropdownOptions}
        forwardedRef={this.values[id].ref}
      />
    );
  };
}

export default function StyledKeyValueDropdownWrapper(props) {
  return (
    <ThemeWrapper>
      <KeyValueDropdownWidget {...props} />
    </ThemeWrapper>
  );
}

(StyledKeyValueDropdownWrapper as any).propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func,
  disabled: PropTypes.bool,
  delimiter: PropTypes.string,
  keyPlaceholder: PropTypes.string,
  kvDelimiter: PropTypes.string,
  dropdownOptions: PropTypes.array,
};
