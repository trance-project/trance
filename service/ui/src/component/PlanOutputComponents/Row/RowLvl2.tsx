import React from "react";

import {rowThemeStyle} from './RowThemeStyle';
import TableRow from "@material-ui/core/TableRow";
import TableCell from "@material-ui/core/TableCell";
import IconButton from "@material-ui/core/IconButton";
import KeyboardArrowUpIcon from "@material-ui/icons/KeyboardArrowUp";
import KeyboardArrowDownIcon from "@material-ui/icons/KeyboardArrowDown";
import Collapse from "@material-ui/core/Collapse";
import Box from "@material-ui/core/Box";
import Typography from "@material-ui/core/Typography";
import Table from "@material-ui/core/Table";
import TableHead from "@material-ui/core/TableHead";
import TableBody from "@material-ui/core/TableBody";

import {mutation} from '../../../utils/Public_Interfaces';
import {rows} from '../DemoData';

interface _RowLvl2Props {
    row:mutation,
    columns?: string[],
    tableName?: string,
}

const RowLvl2 = (props:_RowLvl2Props) => {
    const classes = rowThemeStyle();
    const row = props.row;
    const [open, setOpen] = React.useState(false);
    const styleExpand = row.scores.length>0?classes.expand:classes.noExpand;

    return (
        <React.Fragment>
            <TableRow className={classes.root}>
                <TableCell className={classes.tableCell}>
                    <IconButton aria-label="expand row" size="small" onClick={() => setOpen(!open)} className={styleExpand}>
                        {open ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
                    </IconButton>
                </TableCell>
                <TableCell component="th" scope="row" className={classes.tableCell}>
                    {row.mutId}
                </TableCell>
            </TableRow>
            <TableRow>
                <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={6}>
                    <Collapse in={open} timeout="auto" unmountOnExit>
                        <Box margin={1}>
                            <Typography variant="h6" gutterBottom component="div">
                                Scores
                            </Typography>
                            <Table size="small" aria-label="Mutations">
                                <TableHead>
                                    <TableRow>
                                        <TableCell>Gene</TableCell>
                                        <TableCell>Score</TableCell>
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {row.scores.map((score) => (
                                        <TableRow>
                                            <TableCell>{score.gene}</TableCell>
                                            <TableCell>{score.score}</TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </Box>
                    </Collapse>
                </TableCell>
            </TableRow>
        </React.Fragment>
    )

}

export default RowLvl2