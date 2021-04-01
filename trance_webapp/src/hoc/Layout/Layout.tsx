import React, {useEffect} from "react";
import {useHistory, useLocation} from 'react-router-dom';
import clsx from "clsx";
import CssBaseline from "@material-ui/core/CssBaseline";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import IconButton from "@material-ui/core/IconButton";
import MenuIcon from "@material-ui/icons/Menu";
import AddIcon from '@material-ui/icons/Add';
import Typography from "@material-ui/core/Typography";
import Drawer from "@material-ui/core/Drawer";
import ChevronLeftIcon from "@material-ui/icons/ChevronLeft";
import SearchIcon from '@material-ui/icons/Search';
import Divider from "@material-ui/core/Divider";
import List from "@material-ui/core/List";
import BuildIcon from '@material-ui/icons/Build';
import Container from "@material-ui/core/Container";
import Grid from "@material-ui/core/Grid";
import Box from "@material-ui/core/Box";
import MenuItem from '@material-ui/core/MenuItem';
import Select from '@material-ui/core/Select';
import LayoutThemeStyle from "./LayoutThemeStyle";
import CopyRight from "../../component/CopyRight/CopyRight";
import {Link} from "react-router-dom";
import ListItem from "@material-ui/core/ListItem";
import ListItemIcon from "@material-ui/core/ListItemIcon";
import DashboardIcon from "@material-ui/icons/Dashboard";
import ListItemText from "@material-ui/core/ListItemText";
import DeviceHubIcon from "@material-ui/icons/DeviceHub";
import MapIcon from "@material-ui/icons/Map";
import BarChartIcon from "@material-ui/icons/BarChart";
import NewQueryDialog from "../../component/NewQueryDialog/NewQueryDialog";
import {ListSubheader} from "@material-ui/core";
import AssignmentIcon from "@material-ui/icons/Assignment";
import {pageRoutes} from '../../utils/Public_enums';
import {QuerySummaryList} from '../../utils/Public_Interfaces';
import {useAppDispatch, useAppSelector} from "../../redux/Hooks/hooks";

import {fetchQueryListSummary, fetchSelectedQuery} from '../../redux/QuerySlice/thunkQueryApiCalls';

interface LayoutProps {
    children: React.ReactNode;
    activePage: pageRoutes;
    goto_Route: (goto: pageRoutes) => void;
}


/**
 * Layout
 * @param props
 * Layout of the webapp, this sets the navigation and makes the app bar and left drawer static so
 * will always show on screen and changes the content of the main depending on what route the user is on
 */

const Layout = (props:LayoutProps) => {

    const [open, setOpen] = React.useState(false);
    const [openNewQueryState, setOpenNewQueryState] = React.useState<boolean>(false);

    const selectedQuery = useAppSelector(state => state.query.selectedQuery);
    const queryList = useAppSelector(state => state.query.queryListSummary);

    const location = useLocation();
    const dispatch = useAppDispatch();

    /**
     * Simulate componentDidMount we can get and style to correct path
     * only to run to be run once.
     * called // eslint-disable-next-line to disable warning in console
     */
    useEffect(() => {
        if(location) {
            props.goto_Route(page(location.pathname));
        }
        dispatch(fetchQueryListSummary());

    },
        // eslint-disable-next-line
        []);

    const page = (pathName:string) => {
        switch (pathName){
            case '/report':
                return pageRoutes.REPORT;
            case '/tables':
                return pageRoutes.TABLES;
            case '/queryView':
                return pageRoutes.VIEW;
            case '/builder':
                return pageRoutes.BUILDER;
            case '/':
                return pageRoutes.DASHBOARD;
            default:
                return pageRoutes.DASHBOARD;
        }
    }

        const setSelectedQuery = (querySummary: QuerySummaryList) => {
            dispatch(fetchSelectedQuery(querySummary));
        }

        const handleOpenNewQueryState = () => {
            setOpenNewQueryState(true)
        }

        const handleCloseNewQueryState = () => {
            setOpenNewQueryState(false)
        }

    const history = useHistory();
    const handleNewQuery = (input:string) => {
        // const newQueryList= [input,...queryListState];
        // setQueryListState(newQueryList);
        // setSelectedQuery(newQueryList.findIndex(el=> el===input)+1);
        setOpenNewQueryState(false);
        history.push('/builder')
        props.goto_Route(pageRoutes.BUILDER)
    }


        const handleDrawerOpen = () => {
            setOpen(true);
        }
        const handleDrawerClose = () => {
            setOpen(false);
        }

    const handleChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        const querySummary = queryList.find(el => el.id === event.target.value as string)

        //avoid calling setSelected when None is Selected
        if(querySummary) {
            setSelectedQuery(querySummary);
        }
    };

        const classes = LayoutThemeStyle();

        return(
            <div className={classes.root}>
                <CssBaseline />
                <AppBar position={"absolute"} className={clsx(classes.appBar, open && classes.appBarShift)}>
                    <Toolbar className={classes.toolbar}>
                        <IconButton
                            edge={"start"}
                            color={"inherit"}
                            aria-label={"open drawer"}
                            onClick={handleDrawerOpen}
                            className={clsx(classes.menuButton, open && classes.menuButtonHidden)}>
                            <MenuIcon/>
                        </IconButton>
                        <Typography component={'h1'} variant={'h3'} color={'inherit'} noWrap className={classes.title}>
                            TraNCE
                        </Typography>
                        <div className={classes.search}>
                            <div className={classes.searchIcon}>
                                <SearchIcon />
                            </div>
                            <Select
                                labelId="demo-simple-select-filled-label"
                                id="demo-simple-select-filled"
                                value={selectedQuery? selectedQuery.name: " "}
                                onChange={handleChange}
                                classes={{
                                    root: classes.inputRoot,
                                    select: classes.inputInput,
                                }}
                            >
                                <MenuItem value={" "}>
                                    <em>None</em>
                                </MenuItem>
                                {queryList.map((el, index) => <MenuItem value={el.id} key={index}>{el.name}</MenuItem>)}
                            </Select>
                        </div>
                        <div>
                            <IconButton onClick={handleOpenNewQueryState}>
                                <AddIcon/>
                            </IconButton>
                        </div>
                    </Toolbar>
                </AppBar>
                <Drawer
                    variant={'permanent'}
                    classes={{
                        paper: clsx(classes.drawerPaper, !open && classes.drawerPaperClose),
                    }}
                    open={open}>
                    <div className={classes.toolbarIcon}>
                        <IconButton onClick={handleDrawerClose}>
                            <ChevronLeftIcon />
                        </IconButton>
                    </div>
                    <Divider/>
                    <List className={classes.drawerElement}>
                        <Link to={'/'}>
                            <ListItem className={props.activePage===pageRoutes.DASHBOARD?classes.drawerPaperActive:classes.drawerNav} button onClick={() => props.goto_Route(pageRoutes.DASHBOARD)}>
                                <ListItemIcon >
                                    <DashboardIcon color={"inherit"}/>
                                </ListItemIcon>
                                <ListItemText primary={"Dashboard"} />
                            </ListItem>
                        </Link>
                        <Link to={'/builder'}>
                            <ListItem className={props.activePage===pageRoutes.BUILDER?classes.drawerPaperActive:classes.drawerNav} button onClick={() => props.goto_Route(pageRoutes.BUILDER)}>
                                <ListItemIcon>
                                    <BuildIcon />
                                </ListItemIcon>
                                <ListItemText primary={"Query Builder"} />
                            </ListItem>
                        </Link>
                        <Link to={'/queryView'}>
                            <ListItem className={props.activePage===pageRoutes.VIEW?classes.drawerPaperActive:classes.drawerNav} button onClick={() => props.goto_Route(pageRoutes.VIEW)}>
                                <ListItemIcon>
                                    <DeviceHubIcon />
                                </ListItemIcon>
                                <ListItemText primary={"Compiler"} />
                            </ListItem>
                        </Link>
                        <Link to={'/tables'}>
                            <ListItem className={props.activePage===pageRoutes.TABLES?classes.drawerPaperActive:classes.drawerNav} button onClick={() => props.goto_Route(pageRoutes.TABLES)}>
                                <ListItemIcon>
                                    <MapIcon />
                                </ListItemIcon>
                                <ListItemText primary={"Schema"} />
                            </ListItem>
                        </Link>
                        <Link to={'/report'} className={props.activePage===pageRoutes.REPORT?classes.drawerPaperActive:classes.drawerNav}>
                            <ListItem button onClick={() => props.goto_Route(pageRoutes.REPORT)}>
                                <ListItemIcon>
                                    <BarChartIcon/>
                                </ListItemIcon>
                                <ListItemText primary={"Results"} />
                            </ListItem>
                        </Link>
                    </List>
                    <Divider/>
                    <List>
                        <ListSubheader inset>Recent Activity</ListSubheader>
                        <ListItem button>
                            <ListItemIcon>
                                <AssignmentIcon />
                            </ListItemIcon>
                            <ListItemText primary={"Created GeneLikelihoodPer..."} />
                        </ListItem>
                    </List>
                </Drawer>
                <main className={classes.content}>
                    <div className={classes.appBarSpacer}/>
                    <Container maxWidth={"lg"} className={classes.container}>
                        <Grid container spacing={3}>
                            {props.children}
                        </Grid>
                        <Box pt={4}>
                            <CopyRight/>
                        </Box>
                    </Container>
                </main>
                <NewQueryDialog open={openNewQueryState} close={handleCloseNewQueryState} onClickEvent={handleNewQuery}/>
            </div>
        );
}

export default Layout;