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
} from 'components/AbstractWidget/AbstractMultiRowWidget';

import MultipleValuesRow from 'components/AbstractWidget/MultipleValuesWidget/MultipleValuesRow';
import ThemeWrapper from 'components/ThemeWrapper';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import { objectQuery } from 'services/helpers';

interface IMultipleValuesWidgetProps {
  placeholders?: string[];
  'values-delimiter'?: string;
  numValues: string | number;
  delimiter?: string;
}

interface IMulipleValuesProps extends IMultiRowProps<IMultipleValuesWidgetProps> {}

class MultipleValuesWidgetView extends AbstractMultiRowWidget<IMulipleValuesProps> {
  public renderRow = (id, index) => {
    let numValues = objectQuery(this.props, 'widgetProps', 'numValues');
    if (typeof numValues === 'string') {
      numValues = parseInt(numValues, 10);
      numValues = isNaN(numValues) ? null : numValues;
    }

    const placeholders = objectQuery(this.props, 'widgetProps', 'placeholders');
    const valuesDelimiter = objectQuery(this.props, 'widgetProps', 'values-delimiter');

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
        placeholders={placeholders}
        valuesDelimiter={valuesDelimiter}
        numValues={numValues}
        forwardedRef={this.values[id].ref}
        errors={this.props.errors}
      />
    );
  };
}

export default function MultipleValuesWidget(props) {
  return (
    <ThemeWrapper>
      <MultipleValuesWidgetView {...props} />
    </ThemeWrapper>
  );
}

(MultipleValuesWidget as any).propTypes = WIDGET_PROPTYPES;
(MultipleValuesWidget as any).getWidgetAttributes = () => {
  return {
    placeholders: { type: 'string[]', required: false },
    'values-delimiter': { type: 'string', required: false },
    numValues: { type: 'number', required: true },
    delimiter: { type: 'string', required: false },
  };
};
