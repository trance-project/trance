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

import {AbstractTable, planDemoOutput} from '../../../utils/Public_Interfaces';
import RowLvl2 from "./RowLvl2";
import TableContainer from "@material-ui/core/TableContainer";


interface _RowProps{
    table: AbstractTable;
    show: boolean;
    subTableElement: JSX.Element;
}

const CollapsableTable = (props:_RowProps) => {
    return (
        <React.Fragment>
            <TableRow>
                <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={6}>
                    <Collapse in={props.show} timeout="auto" unmountOnExit>
                        <Box margin={1}>
                            <Typography variant="h6" gutterBottom component="div">
                                {props.table.name}
                            </Typography>
                            <Table size="small" aria-label="Mutations">
                                <TableHead>
                                    <TableRow>
                                        <TableCell/>
                                        {props.table.columnNames.map((e) => <TableCell key={e.concat(Math.random().toString())}>{e}</TableCell>)}
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {props.subTableElement}
                                </TableBody>
                            </Table>
                        </Box>
                    </Collapse>
                </TableCell>
            </TableRow>
        </React.Fragment>
    );
}

export default CollapsableTable;


