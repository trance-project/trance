import React, {useState} from 'react';
import {AppBar, makeStyles, Theme} from "@material-ui/core";
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Box from '@material-ui/core/Box';

import TableConfig from './TableConfig/TableConfig';
import ConditionContrainsts from './ConditionContrainsts/ConditionContrainsts';
import {Column, Query} from "../../Interface/Public_Interfaces";

interface  TabPanelProps {
    children?: React.ReactNode;
    index:any;
    value:any;
}

const TabPanel = (props: TabPanelProps) => {
    const { children, value, index, ...other } = props;
    return (
        <div
            role={"tabpanel"}
            hidden={value !== index}
            aria-labelledby={`scrollable-auto-tab-${index}`}
            {...other}
            >
            {value === index && (
                <Box p={3}>
                    {children}
                </Box>
            )}
        </div>
    );
}

function a11yProps(index: any) {
    return {
        id: `scrollable-auto-tab-${index}`,
        'aria-controls': `scrollable-auto-tabpanel-${index}`,
    };
}

const useStyles = makeStyles((theme: Theme) => ({
    root: {
        flexGrow: 1,
        width: '100%',
        backgroundColor: theme.palette.background.paper,
    },
    appBarSpacer1: theme.mixins.toolbar,
    content: {
        flexGrow: 1,
        height: '100vh',
        overflow: 'auto',
    },
    container1: {
        paddingTop: theme.spacing(4),
        paddingBottom: theme.spacing(4),
    },
    paper1: {
        padding: theme.spacing(2),
        display: 'flex',
        overflow: 'auto',
        flexDirection: 'column',
    },
    fixedHeight1: {
        height: 450,
    },
    button1: {
        margin: theme.spacing(1),
    }
}));

interface _QueryConfigProps {
    query: Query | undefined;
    config: (column:Column) => void;
}

const QueryConfig = (props: _QueryConfigProps) => {
    const classes = useStyles();
    const [value, setValue] = useState(0);

    const handleChange = (event: React.ChangeEvent<{}>, newValue: number) => {
        setValue(newValue);
    }

    const tabPanel = props.query ? (
        <React.Fragment>
            <AppBar position={'static'} color={"default"}>
                <Tabs variant={'scrollable'}
                      onChange={handleChange}
                      indicatorColor={"primary"}
                      textColor={"primary"}
                      scrollButtons={"auto"}
                      aria-label={'scrollable auto tabs'}>
                    <Tab label="Table Config" {...a11yProps(0)} />
                    <Tab label="Conditional Constraints" {...a11yProps(1)} />
                    <Tab label="Group By" {...a11yProps(2)} />
                    <Tab label="Table Joins" {...a11yProps(3)} />
                </Tabs>
            </AppBar>
            <TabPanel value={value} index={0}>
                <TableConfig tables={props.query.tables} columnBoxClicked={props.config}/>
            </TabPanel>
            <TabPanel value={value} index={1}>
                <ConditionContrainsts/>
            </TabPanel>
            <TabPanel value={value} index={2}>
                Item Three
            </TabPanel>
            <TabPanel value={value} index={3}>
                Item Four
            </TabPanel>
        </React.Fragment>
    ): null;

    return (
        <div className={classes.root}>
            {tabPanel}
        </div>
    )
}

export default QueryConfig;