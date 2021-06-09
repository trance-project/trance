import React from 'react';
import Link from '@material-ui/core/Link';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import {ButtonGroup} from "@material-ui/core";
import Button from "@material-ui/core/Button";
import Grid from "@material-ui/core/Grid";
import Paper from "@material-ui/core/Paper";

import overviewThemeStyle from "./OverviewThemeStyle";
import {useAppSelector} from '../../redux/Hooks/hooks';

const preventDefault = (event:  React.MouseEvent<HTMLAnchorElement, MouseEvent>) => event.preventDefault();


const Overview = () => {
    const querySummaryList = useAppSelector(state => state.query.queryListSummary);
    const classes = overviewThemeStyle();
    return (
        <Grid item xs={12} md={12} lg={12}>
            <Paper className={classes.paper}>
                <h2>Recent Queries</h2>
                <Table size="small">
                    <TableHead>
                        <TableRow>
                            {/*<TableCell>Date</TableCell>*/}
                            <TableCell>Name</TableCell>
                            {/*<TableCell>Inputs</TableCell>*/}
                            {/*<TableCell>Compilation</TableCell>*/}
                            <TableCell>Actions</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {querySummaryList.map((row) => (
                            <TableRow key={row._id}>
                                {/*<TableCell>{row.date}</TableCell>*/}
                                <TableCell>{row.name}</TableCell>
                                {/*<TableCell>{row.tables}</TableCell>*/}
                                {/*<TableCell>{row.groupedBy}</TableCell>*/}
                                <TableCell>
                                    <ButtonGroup color={"primary"} aria-label={"Contained primary button group"}>
                                        <Button variant={"contained"} style={{'backgroundColor':'#2980b9'}}>Edit</Button>
                                        <Button variant={"contained"} style={{'backgroundColor':'#e74c3c'}}>Delete</Button>
                                    </ButtonGroup>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
                <div className={classes.seeMore}>
                    <Link color="primary" href="#" onClick={preventDefault}>
                        See more orders
                    </Link>
                </div>
            </Paper>
        </Grid>
    );
}

export default Overview;