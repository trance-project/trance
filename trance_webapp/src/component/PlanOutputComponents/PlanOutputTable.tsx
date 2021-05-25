import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';

import NewRow from "./Row/NewRow";
import {rows} from './DemoData';
import {AbstractTable} from '../../utils/Public_Interfaces';

interface _PlanOutputTableProps {
    onSelect: (tableInfo: AbstractTable, data: any[]) => void;
    tableHeaders?: AbstractTable;
}

const PlanOutputTable = (props: _PlanOutputTableProps) => {

    const element = props.tableHeaders ? props.tableHeaders.columnNames.map((e) => <TableCell key={e.concat(Math.random().toString())}>{e}</TableCell>):null;
    const body = props.tableHeaders ? rows.map((row) => <NewRow table={props.tableHeaders!} nestedObject={row} onSelect={props.onSelect}/>):null;
    return (
        <TableContainer component={Paper}>
            {/*<NewNestedRow rows={rows}/>*/}
            <Table aria-label="collapsible table">
                <TableHead>
                    <TableRow>
                        <TableCell />
                        {element}
                    </TableRow>
                </TableHead>
                <TableBody>
                    {body}
                </TableBody>
            </Table>
        </TableContainer>
    );
}

export default PlanOutputTable;
