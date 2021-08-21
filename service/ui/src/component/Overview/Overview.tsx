import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import {ButtonGroup} from "@material-ui/core";
import Button from "@material-ui/core/Button";
import Grid from "@material-ui/core/Grid";
import Paper from "@material-ui/core/Paper";
import {useHistory} from "react-router-dom";

import overviewThemeStyle from "./OverviewThemeStyle";
import {useAppDispatch, useAppSelector} from '../../redux/Hooks/hooks';
import {fetchSelectedQuery, blocklyDelete, fetchQueryListSummary} from '../../redux/QuerySlice/thunkQueryApiCalls';
import {goToRoute} from '../../redux/NavigationSlice/navigationSlice';
import {pageRoutes} from '../../utils/Public_enums';
import {QuerySummary} from "../../utils/Public_Interfaces";


const preventDefault = (event:  React.MouseEvent<HTMLAnchorElement, MouseEvent>) => event.preventDefault();

/**
 * Overview page to display and summary on all the queries
 * that have been built with blockly and allowing user to edit or delete a query
 * @author Brandon Moore
 * @constructor
 */
const Overview = () => {
    const history = useHistory();
    const dispatch = useAppDispatch();

    const querySummaryList = useAppSelector(state => state.query.queryListSummary);
    const classes = overviewThemeStyle();

    const getAndEdit = (querySummary: QuerySummary) =>{
        dispatch(fetchSelectedQuery(querySummary));
        dispatch(goToRoute(pageRoutes.BUILDER))
        history.push('/builder')
    }

    const deleteHandler = async ( querySummary : QuerySummary) =>{
        //return result of api call then update queryListSummary
        const result = await dispatch(blocklyDelete(querySummary));
        if(result.meta.requestStatus === "fulfilled"){
            dispatch(fetchQueryListSummary());
        }
    }
    return (
        <Grid item xs={12} md={12} lg={12}>
            <Paper className={classes.paper}>
                <h2>Recent Queries</h2>
                <Table size="small">
                    <TableHead>
                        <TableRow>
                            <TableCell>Name</TableCell>
                            <TableCell>Actions</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {querySummaryList.map((row) => (
                            <TableRow key={row._id}>
                                <TableCell>{row.name}</TableCell>
                                <TableCell>
                                    <ButtonGroup color={"primary"} aria-label={"Contained primary button group"}>
                                        <Button variant={"contained"} style={{'backgroundColor':'#2980b9'}} onClick={() => getAndEdit(row)}>Edit</Button>
                                        <Button variant={"contained"} style={{'backgroundColor':'#e74c3c'}} onClick={() => deleteHandler(row)}>Delete</Button>
                                    </ButtonGroup>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </Paper>
        </Grid>
    );
}

export default Overview;