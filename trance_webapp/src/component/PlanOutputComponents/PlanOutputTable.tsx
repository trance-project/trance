import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';

import Row from './Row/Row';
import {rows} from './DemoData';
import {AbstractTable} from '../../utils/Public_Interfaces';

interface _PlanOutputTableProps {
    tableHeaders?: AbstractTable
}

const PlanOutputTable = (props: _PlanOutputTableProps) => {
    console.log("Headers", props.tableHeaders)

    const setTable = (table: AbstractTable) => {
        if(table.subTables && table.subTables.length>0){
            table.subTables.map(t => setTable(t))
        }else{

        }

    }

    // const element = setTable();
    return (
        <TableContainer component={Paper}>

            <Table aria-label="collapsible table">
                <TableHead>
                    <TableRow>
                        <TableCell />
                        <TableCell>Sample</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>

                    {rows.map((row) => (
                        <Row key={row.sample + Math.random().toString()} row={row} />
                    ))}
                </TableBody>
            </Table>
        </TableContainer>
    );
}

export default PlanOutputTable;
