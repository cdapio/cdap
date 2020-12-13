/*
 * Copyright © 2020 Cask Data, Inc.
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
import { IWidgetProps, IStageSchema } from 'components/AbstractWidget';
import { objectQuery } from 'services/helpers';
import MultiSelect from 'components/AbstractWidget/FormInputs/MultiSelect';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';

const styles = () => {
  return {
    emptyMessageContainer: {
      backgroundColor: 'initial',
      padding: 'initial',
    },
    emptyMessage: {
      marginBottom: 0,
      padding: '4px',
    },
  };
};

interface IMultiStageSelectorWidgetProps {
  delimiter: string;
}
interface IMultiStageSelectorProps
  extends IWidgetProps<IMultiStageSelectorWidgetProps>,
    WithStyles<typeof styles> {}

const getInputStages = (inputSchema: IStageSchema[]) => {
  if (!Array.isArray(inputSchema)) {
    return [];
  }
  return inputSchema.map((stage) => ({ id: stage.name, label: stage.name }));
};

const MultiStageSelectorBase: React.FC<IMultiStageSelectorProps> = ({
  value,
  onChange,
  disabled,
  extraConfig,
  widgetProps,
  classes,
}) => {
  const inputSchema = objectQuery(extraConfig, 'inputSchema');
  const inputStages = getInputStages(inputSchema);
  const { delimiter } = widgetProps;
  const multiSelectWidgetProps = {
    delimiter,
    options: inputStages,
    showSelectionCount: false,
    emptyPlaceholder: 'Select input stage',
  };
  if (!inputStages.length) {
    return (
      <div className={classes.emptyMessageContainer}>
        <div className={classes.emptyMessage}>No input stages</div>
      </div>
    );
  }
  const validatedValue = value
    .toString()
    .split(',')
    .filter((stage) => {
      return inputStages.find((s) => s.id === stage);
    })
    .join(',');
  if (validatedValue !== value) {
    /**
     * This will happen when the user configured the plugin (say joiner)
     * with initial two stages and then later drop one of those stages.
     *
     * Now the previously selected input stages are no longer valid.
     * The validatedValue will only have stages that are still input to joiner
     * and will update the value of the plugin automatically.
     *
     * If not for this we will show only valid stages but the actual plugin property
     * will have all the stages.
     */
    onChange(validatedValue);
  }
  return (
    <React.Fragment>
      <MultiSelect
        value={validatedValue}
        onChange={onChange}
        widgetProps={multiSelectWidgetProps}
        disabled={disabled}
      />
    </React.Fragment>
  );
};

const StyledMultiStageSelector = withStyles(styles)(MultiStageSelectorBase);
export default function MultiStageSelector(props) {
  return (
    <ThemeWrapper>
      <StyledMultiStageSelector {...props} />
    </ThemeWrapper>
  );
}
(MultiStageSelector as any).propTypes = WIDGET_PROPTYPES;
