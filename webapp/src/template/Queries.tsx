import React from 'react';
import Link from '@material-ui/core/Link';
import { makeStyles } from '@material-ui/core/styles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import {ButtonGroup} from "@material-ui/core";
import Button from "@material-ui/core/Button";

const createData = (id: number, date: string, name:string, tables: string, groupedBy: string) => ({id, date, name, tables, groupedBy, });


const rows = [
    createData(0, '16 Mar, 2021', 'Query_1', 'Table_1,Table_5', 'Column_2'),
    createData(1, '16 Mar, 2021', 'Query_2', 'Table_3,Table_4,Table_2,Table_11', 'Column_2, Column_4'),
    createData(2, '16 Mar, 2021', 'Query_3', 'Table_1,Table_2', 'Column_2, Column_1, Column_5'),
    createData(3, '16 Mar, 2021', 'Query_4', 'Table_21,Table_5', 'Column_1'),
    createData(4, '15 Mar, 2021', 'Query_5', 'Table_41,Table_21,Table_10', 'Column_6'),
    createData(5, '16 Mar, 2021', 'Query_1', 'Table_1,Table_5', 'Column_2'),
    createData(6, '16 Mar, 2021', 'Query_2', 'Table_3,Table_4,Table_2,Table_11', 'Column_2, Column_4'),
    createData(7, '16 Mar, 2021', 'Query_3', 'Table_1,Table_2', 'Column_2, Column_1, Column_5'),
    createData(8, '16 Mar, 2021', 'Query_4', 'Table_21,Table_5', 'Column_1'),
    createData(9, '15 Mar, 2021', 'Query_5', 'Table_41,Table_21,Table_10', 'Column_6'),
    createData(10, '16 Mar, 2021', 'Query_1', 'Table_1,Table_5', 'Column_2'),
    createData(11, '16 Mar, 2021', 'Query_2', 'Table_3,Table_4,Table_2,Table_11', 'Column_2, Column_4'),
    createData(12, '16 Mar, 2021', 'Query_3', 'Table_1,Table_2', 'Column_2, Column_1, Column_5'),
    createData(13, '16 Mar, 2021', 'Query_4', 'Table_21,Table_5', 'Column_1'),
    createData(14, '15 Mar, 2021', 'Query_5', 'Table_41,Table_21,Table_10', 'Column_6'),
    createData(15, '16 Mar, 2021', 'Query_1', 'Table_1,Table_5', 'Column_2'),
    createData(16, '16 Mar, 2021', 'Query_2', 'Table_3,Table_4,Table_2,Table_11', 'Column_2, Column_4'),
    createData(17, '16 Mar, 2021', 'Query_3', 'Table_1,Table_2', 'Column_2, Column_1, Column_5'),
    createData(18, '16 Mar, 2021', 'Query_4', 'Table_21,Table_5', 'Column_1'),
    createData(19, '15 Mar, 2021', 'Query_5', 'Table_41,Table_21,Table_10', 'Column_6'),
    createData(20, '16 Mar, 2021', 'Query_1', 'Table_1,Table_5', 'Column_2'),
    createData(21, '16 Mar, 2021', 'Query_2', 'Table_3,Table_4,Table_2,Table_11', 'Column_2, Column_4'),
    createData(22, '16 Mar, 2021', 'Query_3', 'Table_1,Table_2', 'Column_2, Column_1, Column_5'),
    createData(23, '16 Mar, 2021', 'Query_4', 'Table_21,Table_5', 'Column_1'),
    createData(24, '15 Mar, 2021', 'Query_5', 'Table_41,Table_21,Table_10', 'Column_6'),
];

const preventDefault = (event:  React.MouseEvent<HTMLAnchorElement, MouseEvent>) => event.preventDefault();

const useStyle = makeStyles((theme) => ({
    seeMore: {
        marginTop: theme.spacing(3)
    }
}));

const Queries = () => {
    const classes = useStyle();
    return (
        <React.Fragment>
        <h2>Recent Queries</h2>
            <Table size="small">
                <TableHead>
                    <TableRow>
                        <TableCell>Date</TableCell>
                        <TableCell>Name</TableCell>
                        <TableCell>Tables</TableCell>
                        <TableCell>Group By</TableCell>
                        <TableCell>Actions</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {rows.map((row) => (
                        <TableRow key={row.id}>
                            <TableCell>{row.date}</TableCell>
                            <TableCell>{row.name}</TableCell>
                            <TableCell>{row.tables}</TableCell>
                            <TableCell>{row.groupedBy}</TableCell>
                            <TableCell>
                                <ButtonGroup color={"primary"} aria-label={"Contained primary button group"}>
                                    <Button variant={"contained"} style={{'backgroundColor':'#2980b9'}}>Edit</Button>
                                    <Button variant={"contained"} style={{'backgroundColor':'#2ecc71'}}>Execute</Button>
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
        </React.Fragment>
    );
}

export default Queries;