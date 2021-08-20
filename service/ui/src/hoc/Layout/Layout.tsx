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
import {NavLink} from "react-router-dom";
import ListItem from "@material-ui/core/ListItem";
import ListItemIcon from "@material-ui/core/ListItemIcon";
import DashboardIcon from "@material-ui/icons/Dashboard";
import ListItemText from "@material-ui/core/ListItemText";
import DeviceHubIcon from "@material-ui/icons/DeviceHub";
import BarChartIcon from "@material-ui/icons/BarChart";
import NewQueryDialog from "../../component/NewQueryDialog/NewQueryDialog";
import {pageRoutes} from '../../utils/Public_enums';
import {QuerySummary} from '../../utils/Public_Interfaces';
import {useAppDispatch, useAppSelector} from "../../redux/Hooks/hooks";

import {fetchQueryListSummary, fetchSelectedQuery, blocklyCreateNew} from '../../redux/QuerySlice/thunkQueryApiCalls';
import {fetchTranceObjectList} from '../../redux/TranceObjectSlice/thunkTranceObjectApiCalls';

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
    const [selectedQueryIndexState, setSelectedQueryState] = React.useState<string>(" ");

    const queryList = useAppSelector(state => state.query.queryListSummary);
    const querySelected = useAppSelector(state => state.query.selectedQuery);
    const [standardPlan, shreddedResponse] = useAppSelector(state => [state.query.standardPlan, state.query.shreddedResponse]);


    /**
     * Query response that the backend sent back in a json format to used to display
     */
    const queryResponse = useAppSelector(state => state.query.responseQuery);

    const location = useLocation();
    const dispatch = useAppDispatch();

    const handleNavClick = (e: React.MouseEvent<HTMLAnchorElement, MouseEvent>, verify: "query"|"plan"|"output") => {
        switch (verify) {
            case "query":{
                if(!querySelected){
                    e.preventDefault();
                }else{
                    props.goto_Route(pageRoutes.BUILDER)
                }
                break;
            }
            case "plan":{
                if(!queryResponse){
                    e.preventDefault();
                }else{
                    props.goto_Route(pageRoutes.VIEW)
                }
                break;
            }
            case "output": {
                if(standardPlan || shreddedResponse){
                    props.goto_Route(pageRoutes.REPORT)
                }else{
                    e.preventDefault();
                }
                break;
            }

        }
    }

    /**
     * Simulate componentDidMount we can get and style to correct path
     * only to be run once.
     * called // eslint-disable-next-line to disable warning in console
     */
    useEffect(() => {
        if(location) {
            console.log("[location]", location.pathname)
            switch (location.pathname){
                case "/builder" :{
                    if(!querySelected){
                        props.goto_Route(pageRoutes.DASHBOARD);
                        history.push('/');
                    }
                    break;
                }
                case "/queryView" :{
                    if(!queryResponse){
                        props.goto_Route(pageRoutes.DASHBOARD);
                        history.push('/');
                    }
                    break;
                }
                case "/report" :{
                    if(!standardPlan){
                        props.goto_Route(pageRoutes.DASHBOARD);
                        history.push('/');
                    }
                    break;
                }
            }
        }
        dispatch(fetchQueryListSummary());
        dispatch(fetchTranceObjectList());

    },
        // eslint-disable-next-line
        []);

    /**
     * Simulate componentDidUpdate And checks if a query has been selected
     */
    useEffect(() => {
           const index = queryList.findIndex(el => {
                if(querySelected){
                    return el._id === querySelected._id;
                }
            });
            if(index >= 0){
                setSelectedQueryState(`${index}`);
            }else{
                setSelectedQueryState(" ");
            }
        },
        [querySelected,queryList]);

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

        const setSelectedQuery = (querySummary: QuerySummary) => {
            dispatch(fetchSelectedQuery(querySummary));
        }

        const handleOpenNewQueryState = () => {
            setOpenNewQueryState(true)
        }

        const handleCloseNewQueryState = () => {
            setOpenNewQueryState(false)
        }

    const history = useHistory();
    const handleNewQuery = async (input:string) => {
        const newQuery: QuerySummary = {
            name: input,
            xmlDocument: '<xml xmlns="http://www.w3.org/1999/xhtml"></xml>'
        }

        //return result of api call then update queryListSummary
        const result = await dispatch(blocklyCreateNew(newQuery));
        if(result.meta.requestStatus === "fulfilled"){
            dispatch(fetchQueryListSummary());
        }
        setOpenNewQueryState(false);
        // history.push('/builder')
        // props.goto_Route(pageRoutes.BUILDER)
    }


        const handleDrawerOpen = () => {
            setOpen(true);
        }
        const handleDrawerClose = () => {
            setOpen(false);
        }

    const handleChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        const value = event.target.value as string;
        if(value === " "){
            setSelectedQueryState(value);
            return;
        }
        const querySummary = queryList[event.target.value as number];

        if(querySummary) {
            setSelectedQuery(querySummary);
            history.push('/builder')
            props.goto_Route(pageRoutes.BUILDER)
        }
        setSelectedQueryState(value);
    };

        const classes = LayoutThemeStyle();
    console.log("[LAYOUT querySelected]", querySelected);
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
                                value={selectedQueryIndexState}
                                onChange={handleChange}
                                classes={{
                                    root: classes.inputRoot,
                                    select: classes.inputInput,
                                }}
                            >
                                <MenuItem value={" "}>
                                    <em>None</em>
                                </MenuItem>
                                {queryList.map((el, index) => <MenuItem value={index}  key={index}>{el.name}</MenuItem>)}
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
                        <NavLink to={'/'}>
                            <ListItem className={props.activePage===pageRoutes.DASHBOARD?classes.drawerPaperActive:classes.drawerNav} button onClick={() => props.goto_Route(pageRoutes.DASHBOARD)}>
                                <ListItemIcon >
                                    <DashboardIcon color={"inherit"}/>
                                </ListItemIcon>
                                <ListItemText primary={"Dashboard"} />
                            </ListItem>
                        </NavLink>
                        <NavLink to={'/builder'} onClick={(e) => handleNavClick(e,"query")} className={!querySelected?classes.drawerNavDisable:""}>
                            <ListItem className={props.activePage===pageRoutes.BUILDER?classes.drawerPaperActive:classes.drawerNav} button>
                                <ListItemIcon>
                                    <BuildIcon />
                                </ListItemIcon>
                                <ListItemText primary={"Query Builder"} />
                            </ListItem>
                        </NavLink>
                        <NavLink to={'/queryView'} onClick={(e) => handleNavClick(e,"plan")} className={!queryResponse?classes.drawerNavDisable:""}>
                            <ListItem className={props.activePage===pageRoutes.VIEW?classes.drawerPaperActive:classes.drawerNav} button>
                                <ListItemIcon>
                                    <DeviceHubIcon />
                                </ListItemIcon>
                                <ListItemText primary={"Compiler"} />
                            </ListItem>
                        </NavLink>
                         {/*TODO Add the schema information*/}
                        {/*<Link to={'/tables'}>*/}
                        {/*    <ListItem className={props.activePage===pageRoutes.TABLES?classes.drawerPaperActive:classes.drawerNav} button onClick={() => props.goto_Route(pageRoutes.TABLES)}>*/}
                        {/*        <ListItemIcon>*/}
                        {/*            <MapIcon />*/}
                        {/*        </ListItemIcon>*/}
                        {/*        <ListItemText primary={"Schema"} />*/}
                        {/*    </ListItem>*/}
                        {/*</Link>*/}
                        <NavLink to={'/report'} onClick={(e) => handleNavClick(e,"output")} className={standardPlan || shreddedResponse?"":classes.drawerNavDisable}>
                            <ListItem className={props.activePage===pageRoutes.REPORT?classes.drawerPaperActive:classes.drawerNav} button>
                                <ListItemIcon>
                                    <BarChartIcon/>
                                </ListItemIcon>
                                <ListItemText primary={"Results"} />
                            </ListItem>
                        </NavLink>
                    </List>
                    <Divider/>
                    {/*TODO add recent activity action of users action*/}
                    {/*<List>*/}
                    {/*    <ListSubheader inset>Recent Activity</ListSubheader>*/}
                    {/*    <ListItem button>*/}
                    {/*        <ListItemIcon>*/}
                    {/*            <AssignmentIcon />*/}
                    {/*        </ListItemIcon>*/}
                    {/*        <ListItemText primary={"Created GeneLikelihoodPer..."} />*/}
                    {/*    </ListItem>*/}
                    {/*</List>*/}
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