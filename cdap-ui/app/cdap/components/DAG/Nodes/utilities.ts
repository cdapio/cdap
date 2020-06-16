import { StyleRules } from '@material-ui/core/styles/withStyles';
import { bluegrey } from 'components/ThemeWrapper/colors';
const ENDPOINT_RADIUS = 7;

const nodeStyles = (theme, customNodeStyles = {}): StyleRules => {
  return {
    root: {
      height: '100px',
      width: '200px',
      border: '1px solid black',
      display: 'inline-flex',
      alignItems: 'center',
      justifyContent: 'center',
      position: 'absolute',
      overflow: 'visible',
      borderRadius: '4px',
      // TODO
      // '&:hover': {
      //   borderWidth: '4px',
      //   margin: '-2px',
      //   height: '104px',
      //   width: '204px',
      // },
      ...customNodeStyles,
    },
    focusVisible: {},
  };
};

const endpointCircle = (theme): StyleRules => {
  return {
    root: {
      width: `${ENDPOINT_RADIUS * 2}px`,
      height: `${ENDPOINT_RADIUS * 2}px`,
      backgroundColor: theme.palette.bluegrey[100],
      borderRadius: '100%',
      position: 'absolute',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      '&.hover': {
        backgroundColor: `${theme.palette.blue[300]}`,
        '&:before': {
          display: 'block',
        },
        '&:after': {
          display: 'block',
        },
        '& *': {
          visibility: 'hidden',
        },
        '& [data-type="endpoint-label"]': {
          visibility: 'visible',
        },
      },
      '&:before': {
        content: '""',
        position: 'absolute',
        left: '100%',
        width: '5px',
        borderBottom: `2px solid ${theme.palette.blue[300]}`,
        display: 'none',
      },
      '&:after': {
        content: '""',
        width: 0,
        height: 0,
        borderTop: `${ENDPOINT_RADIUS}px solid transparent`,
        borderBottom: `${ENDPOINT_RADIUS}px solid transparent`,
        borderLeft: `${ENDPOINT_RADIUS}px solid ${theme.palette.blue[300]}`,
        transform: 'translateX(12px)',
        display: 'none',
      },
    },
    bottomEndpointCircle: {
      '&:before': {
        left: '6px',
        top: '100%',
        borderBottom: 'none',
        borderLeft: `2px solid ${theme.palette.blue[300]}`,
        height: '5px',
      },
      '&:after': {
        content: '""',
        width: 0,
        height: 0,
        borderBottom: '7px solid transparent',
        borderTop: `7px solid ${theme.palette.blue[300]}`,
        borderLeft: '7px solid transparent',
        borderRight: '7px solid transparent',
        transform: 'translate(-4px, 19px)',
      },
    },
    bottomEndpointLabel: {
      position: 'absolute',
      top: '-18px',
      color: theme.palette.grey[300],
    },
    regularEndpointCircle: {
      right: `-${ENDPOINT_RADIUS}px`,
      top: '41px',
    },
    alertEndpointCircle: {
      bottom: `-${ENDPOINT_RADIUS}px`,
      left: `${ENDPOINT_RADIUS * 2}px`,
    },
    errorEndpointCircle: {
      bottom: `-${ENDPOINT_RADIUS}px`,
      left: `${ENDPOINT_RADIUS * 7}px`,
    },
    conditionNoEndpointCircle: {
      bottom: `-${ENDPOINT_RADIUS}px`,
      left: `${105 / 2 - 7}px`,
    },
  };
};
const endpointCaret = (theme): StyleRules => ({
  root: {
    width: 0,
    height: 0,
    borderTop: '4px solid transparent',
    borderBottom: '4px solid transparent',
    borderLeft: `${ENDPOINT_RADIUS}px solid #bac0d6`,
    transform: 'translateX(1px)',
  },
});

const bottomEndpointCaret = (): StyleRules => ({
  root: {
    width: 0,
    height: 0,
    borderLeft: '4px solid transparent',
    borderRight: '4px solid transparent',
    borderTop: '7px solid #bac0d6',
    transform: 'translateY(2px)',
  },
});
const endpointPaintStyles = {
  width: 20,
  height: 20,
  connectorStyle: {
    lineWidth: 2,
    outlineColor: 'transparent',
    outlineWidth: 4,
    strokeStyle: bluegrey[100],
    strokeWidth: 3,
  },
  fill: 'black',
  lineWidth: 3,
  radius: 10,
  stroke: 'black',
};

const endpointTargetEndpointParams = (endpointId) => {
  return {
    allowLoopback: false,
    anchor: 'ContinuousLeft',
    dropOptions: { hoverClass: 'drag-hover' },
    isTarget: true,
    uuid: endpointId,
    type: 'dashed',
  };
};

const genericNodeStyles = (customNodeStyles = {}, customStyles = {}) => {
  return (theme): StyleRules => {
    const endpointCircleStyles = endpointCircle(theme);
    return {
      root: {
        ...nodeStyles(theme, customNodeStyles).root,
      },
      endpointCircle: {
        ...endpointCircleStyles.root,
      },
      bottomEndpointCircle: {
        ...endpointCircleStyles.bottomEndpointCircle,
      },
      bottomEndpointLabel: {
        ...endpointCircleStyles.bottomEndpointLabel,
      },
      regularEndpointCircle: {
        ...endpointCircleStyles.regularEndpointCircle,
      },
      alertEndpointCircle: {
        ...endpointCircleStyles.alertEndpointCircle,
      },
      errorEndpointCircle: {
        ...endpointCircleStyles.errorEndpointCircle,
      },
      conditionNoEndpointCircle: {
        ...endpointCircleStyles.conditionNoEndpointCircle,
      },
      endpointCaret: {
        ...endpointCaret(theme).root,
      },
      bottomEndpointCaret: {
        ...bottomEndpointCaret().root,
      },
      ...customStyles,
    };
  };
};

export {
  endpointCircle,
  endpointCaret,
  endpointPaintStyles,
  endpointTargetEndpointParams,
  nodeStyles,
  genericNodeStyles,
};
