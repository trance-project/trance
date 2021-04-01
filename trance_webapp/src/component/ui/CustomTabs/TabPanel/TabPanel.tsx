import React from 'react';
import Typography from '@material-ui/core/Typography';
import Box from '@material-ui/core/Box';

interface  _TabPanelProps {
    children?: React.ReactNode;
    dir?: string;
    index: any;
    value: any;
}
const TabPanel = (props:_TabPanelProps) => {
    const { children, value, index, ...other } = props;

    return (
        <div
            role={'tabpanel'}
            hidden={props.value !== index}
            id={`full-width-tabpanel-${props.index}`}
            {...other}
            >
            {value === index && (
                <Box p={3}>
                    <Typography component={"div"}>{children}</Typography>
                </Box>
            )}
        </div>
    );
};

export default TabPanel;