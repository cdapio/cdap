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

import AbstractMultiRowWidget, {
  IMultiRowProps,
  IMultiRowWidgetProps,
} from 'components/AbstractWidget/AbstractMultiRowWidget';

import CSVRow from 'components/AbstractWidget/CSVWidget/CSVRow';
import ThemeWrapper from 'components/ThemeWrapper';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import { objectQuery } from 'services/helpers';

interface ICSVWidgetProps extends IMultiRowWidgetProps {
  'value-placeholder'?: string;
  delimiter?: string;
}

interface ICSVProps extends IMultiRowProps<ICSVWidgetProps> {}

class CSVWidgetView extends AbstractMultiRowWidget<ICSVProps> {
  public renderRow = (id, index) => {
    const valuePlaceholder = objectQuery(this.props, 'widgetProps', 'value-placeholder');

    return (
      <CSVRow
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
        valuePlaceholder={valuePlaceholder}
        forwardedRef={this.values[id].ref}
        errors={this.props.errors}
      />
    );
  };
}

export default function CSVWidget(props) {
  return (
    <ThemeWrapper>
      <CSVWidgetView {...props} />
    </ThemeWrapper>
  );
}

(CSVWidget as any).propTypes = WIDGET_PROPTYPES;
(CSVWidget as any).getWidgetAttributes = () => {
  return {
    'value-placeholder': { type: 'string', required: false },
    delimiter: { type: 'string', required: false },
    default: { type: 'string', required: false },
  };
};
