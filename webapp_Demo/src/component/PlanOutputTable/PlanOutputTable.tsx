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

function createData(
    name: string,
    calories: number,
    fat: number,
    carbs: number,
    protein: number,
    price: number,
) {
    return {
        name,
        calories,
        fat,
        carbs,
        protein,
        price,
        history: [
            { date: '2020-01-05', customerId: '11091700', amount: 3 },
            { date: '2020-01-02', customerId: 'Anonymous', amount: 1 },
        ],
    };
}

type planType = {
    sample: string;
    mutations: [{
        mutId:string, scores:[{
            gene:string,
            score:number
        }]
    }]
}

const PlanOutputTable = () => (
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

export default PlanOutputTable;


// {sample:"1f971af1-6772-4fe6-8d35-bbe527a037fe", mutations:[
//     {mutId:"2233382993923",
//         scores:[{
//             gene:"ENSG00000175294",
//             score:0.0
//         }]
//     }
// ]},