import React from 'react';
import PropTypes from 'prop-types';
import { objectQuery } from 'services/helpers';

import ToggleSwitch from 'components/ToggleSwitch';

interface IToggle {
  label: string;
  value: string;
}
interface IWidgetAttributes {
  on: IToggle;
  off: IToggle;
  default?: string;
}
interface IToggleSwitchWidgetProps {
  value: string;
  widgetAttributes: IWidgetAttributes;
  disabled: boolean;
  onChange: (value: string) => void;
}

const ToggleWidget: React.FC<IToggleSwitchWidgetProps> = ({
  widgetAttributes,
  value,
  onChange,
  disabled,
}) => {
  const onValue = objectQuery(widgetAttributes, 'on', 'value') || 'on';
  const offValue = objectQuery(widgetAttributes, 'off', 'value') || 'off';
  const defaultValue = objectQuery(widgetAttributes, 'default') || onValue;
  const onLabel = objectQuery(widgetAttributes, 'on', 'label') || 'On';
  const offLabel = objectQuery(widgetAttributes, 'off', 'label') || 'Off';
  const model = value || defaultValue;
  const isOn = model === onValue;

  function toggleSwitch() {
    onChange(isOn ? offValue : onValue);
  }
  return (
    <ToggleSwitch
      isOn={isOn}
      onToggle={toggleSwitch}
      disabled={disabled}
      onLabel={onLabel}
      offLabel={offLabel}
    />
  );
};
export default ToggleWidget;
(ToggleWidget as any).propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func,
  widgetAttributes: PropTypes.object,
  disabled: PropTypes.bool,
};
