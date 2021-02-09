import React from 'react';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import ListSubheader from '@material-ui/core/ListSubheader';
import DashboardIcon from '@material-ui/icons/Dashboard';
import DeveloperModeIcon from '@material-ui/icons/DeveloperMode';
import MapIcon from '@material-ui/icons/Map';
import BarChartIcon from '@material-ui/icons/BarChart';
import AssignmentIcon from '@material-ui/icons/Assignment';
import {Link} from 'react-router-dom';

export const mainListItems = (
    <div>
        <Link to={'/'}>
            <ListItem button>
                <ListItemIcon>
                    <DashboardIcon />
                </ListItemIcon>
                <ListItemText primary={"Dashboard"} />
            </ListItem>
        </Link>
        <Link to={'/builder'}>
            <ListItem button>
                <ListItemIcon>
                    <DeveloperModeIcon />
                </ListItemIcon>
                <ListItemText primary={"Query Builder"} />
            </ListItem>
        </Link>
        <Link to={'/tables'}>
            <ListItem button>
                <ListItemIcon>
                    <MapIcon />
                </ListItemIcon>
                <ListItemText primary={"Schema Overview"} />
            </ListItem>
        </Link>
        <Link to={'/report'}>
            <ListItem button>
                <ListItemIcon>
                    <BarChartIcon/>
                </ListItemIcon>
                <ListItemText primary={"Reports"} />
            </ListItem>
        </Link>
    </div>
);

export const secondaryListItems = (
    <div>
        <ListSubheader inset>Recent Activity</ListSubheader>
        <ListItem button>
            <ListItemIcon>
                <AssignmentIcon />
            </ListItemIcon>
            <ListItemText primary={"Created Query demo_1"} />
        </ListItem>
        <ListItem button>
            <ListItemIcon>
                <AssignmentIcon />
            </ListItemIcon>
            <ListItemText primary={"Created Query demo_2"} />
        </ListItem>
        <ListItem button>
            <ListItemIcon>
                <AssignmentIcon />
            </ListItemIcon>
            <ListItemText primary={"Created Query demo_3"} />
        </ListItem>
    </div>
);