import React from "react";
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
import {mainListItems, secondaryListItems} from "../../template/listItems";
import Container from "@material-ui/core/Container";
import Grid from "@material-ui/core/Grid";
import Box from "@material-ui/core/Box";
import MenuItem from '@material-ui/core/MenuItem';
import Select from '@material-ui/core/Select';

import LayoutThemeStyle from "./LayoutThemeStyle";
import CopyRight from "../../component/CopyRight/CopyRight";
import image from '../../static/images/planOperator/outer-unest.png';
import {Link} from "react-router-dom";
import ListItem from "@material-ui/core/ListItem";
import ListItemIcon from "@material-ui/core/ListItemIcon";
import DashboardIcon from "@material-ui/icons/Dashboard";
import ListItemText from "@material-ui/core/ListItemText";
import DeviceHubIcon from "@material-ui/icons/DeviceHub";
import CodeIcon from "@material-ui/icons/Code";
import MapIcon from "@material-ui/icons/Map";
import BarChartIcon from "@material-ui/icons/BarChart";

interface LayoutProps {
    children: React.ReactNode;
}

enum pageRoutes {
    DASHBOARD,
    VIEW,
    BUILDER,
    TABLES,
    REPORT
}

const Layout = (props:LayoutProps) => {
        const [open, setOpen] = React.useState(false);
        const [selectedQuery, setSelectedQuery] = React.useState('10');
        const [activePageState, setActivePageState] = React.useState<pageRoutes>(pageRoutes.DASHBOARD);


        const handleDrawerOpen = () => {
            setOpen(true);
        }
        const handleDrawerClose = () => {
            setOpen(false);
        }

    const handleChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        setSelectedQuery(event.target.value as string);
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
                                value={selectedQuery}
                                onChange={handleChange}
                                classes={{
                                    root: classes.inputRoot,
                                    select: classes.inputInput,
                                }}
                            >
                                <MenuItem value="">
                                    <em>None</em>
                                </MenuItem>
                                <MenuItem value={10}>Biomedical Query</MenuItem>
                                <MenuItem value={20}>Test 1</MenuItem>
                                <MenuItem value={30}>Test 2</MenuItem>
                            </Select>
                        </div>
                        <div>
                            <AddIcon/>
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
                            <ListItem className={activePageState===pageRoutes.DASHBOARD?classes.drawerPaperActive:classes.drawerNav} button onClick={() => setActivePageState(pageRoutes.DASHBOARD)}>
                                <ListItemIcon >
                                    <DashboardIcon color={"inherit"}/>
                                </ListItemIcon>
                                <ListItemText primary={"Dashboard"} />
                            </ListItem>
                        </Link>
                        <Link to={'/queryView'}>
                            <ListItem className={activePageState===pageRoutes.VIEW?classes.drawerPaperActive:classes.drawerNav} button onClick={() => setActivePageState(pageRoutes.VIEW)}>
                                <ListItemIcon>
                                    <DeviceHubIcon />
                                </ListItemIcon>
                                <ListItemText primary={"Query View"} />
                            </ListItem>
                        </Link>
                        <Link to={'/builder'}>
                            <ListItem className={activePageState===pageRoutes.BUILDER?classes.drawerPaperActive:classes.drawerNav} button onClick={() => setActivePageState(pageRoutes.BUILDER)}>
                                <ListItemIcon>
                                    <CodeIcon />
                                </ListItemIcon>
                                <ListItemText primary={"Query Builder"} />
                            </ListItem>
                        </Link>
                        <Link to={'/tables'}>
                            <ListItem className={activePageState===pageRoutes.TABLES?classes.drawerPaperActive:classes.drawerNav} button onClick={() => setActivePageState(pageRoutes.TABLES)}>
                                <ListItemIcon>
                                    <MapIcon />
                                </ListItemIcon>
                                <ListItemText primary={"Schema Overview"} />
                            </ListItem>
                        </Link>
                        <Link to={'/report'} className={activePageState===pageRoutes.REPORT?classes.drawerPaperActive:classes.drawerNav}>
                            <ListItem button onClick={() => setActivePageState(pageRoutes.REPORT)}>
                                <ListItemIcon>
                                    <BarChartIcon/>
                                </ListItemIcon>
                                <ListItemText primary={"Reports"} />
                            </ListItem>
                        </Link>
                    </List>
                    <Divider/>
                    <List>{secondaryListItems}</List>
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
            </div>
        );
}

export default Layout;