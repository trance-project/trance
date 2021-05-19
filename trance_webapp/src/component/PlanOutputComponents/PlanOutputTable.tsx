import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';

import Row from './Row/Row';
import NewRow from "./Row/NewRow";
import {rows} from './DemoData';
import {AbstractTable} from '../../utils/Public_Interfaces';
import IconButton from "@material-ui/core/IconButton";
import KeyboardArrowUpIcon from "@material-ui/icons/KeyboardArrowUp";
import KeyboardArrowDownIcon from "@material-ui/icons/KeyboardArrowDown";
import Collapse from "@material-ui/core/Collapse";
import NewNestedRow from "./Row/NewNestedRow";
import CollapsableTable from "./Row/CollapsableTable";
import Box from "@material-ui/core/Box";
import Typography from "@material-ui/core/Typography";
import {rowThemeStyle} from "./Row/RowThemeStyle";

interface _PlanOutputTableProps {
    tableHeaders?: AbstractTable
}

const PlanOutputTable = (props: _PlanOutputTableProps) => {
    console.log("Headers", props.tableHeaders)
    const classes = rowThemeStyle();
    const styleExpand = classes.expand;

    const processTable = (table: AbstractTable, nestedObject: any) => {
        const elementCollapseRow: JSX.Element[] = [];
        const elementColumn: JSX.Element[] = [];
        let hasNestedElement = false;
        for( let [key] of Object.entries(nestedObject)){
            if(key === table.subTables?.name){
                hasNestedElement = nestedObject[key].length>0;
                elementCollapseRow.push(setCollapsableTable(table.subTables, nestedObject[key]))
            }else{
                elementColumn.push((
                    <TableCell style={{paddingBottom: 0, paddingTop: 0}} colSpan={6}>
                        {nestedObject[key]}
                    </TableCell>
                ))
            }
        }
        return (
            <NewRow elementNested={hasNestedElement} elementColumn={elementColumn} elementCollapseRow={elementCollapseRow}/>
        );
    }

    const processTable2 = (table: AbstractTable, nestedObject: any) => {
        console.log("[processTable2]", nestedObject)
        const elementCollapseRow: JSX.Element[] = [];
        const elementColumn: JSX.Element[] = [];
        for( let [key] of Object.entries(nestedObject)){
            if(key !== table.subTables?.name)
                elementColumn.push((
                    <TableCell style={{paddingBottom: 0, paddingTop: 0}} colSpan={6}>
                        {nestedObject[key]}
                    </TableCell>
                ))
        }
        return (
            <React.Fragment>
                <TableRow>
                    <TableCell className={classes.tableCell}>
                        <IconButton aria-label="expand row" size="small" onClick={() => {}} className={styleExpand}>
                            {<KeyboardArrowUpIcon />}
                        </IconButton>
                    </TableCell>
                    {elementColumn}
                </TableRow>
                {elementCollapseRow}
            </React.Fragment>
        );
    }

    const setCollapsableTable = (table: AbstractTable, nestedObject: any[]) => {
        console.log("[setCollapsableTable]", nestedObject[0])
        const heading = table.columnNames.map((e) => <TableCell key={e.concat(Math.random().toString())}>{e}</TableCell>);
       const subTableElement = nestedObject.map(o => {
            if(table.subTables){
                return processTable(table, o)
            }else{
                return processTable(table, o)
            }
        })

        return (
            <TableRow>
                <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={6}>
                    <Collapse in={true} timeout="auto" unmountOnExit>
                        <Box margin={1}>
                            <Typography variant="h6" gutterBottom component="div">
                                {table.name}
                            </Typography>
                            <Table size="small" aria-label="Mutations">
                                <TableHead>
                                    <TableRow>
                                        <TableCell/>
                                        {heading}
                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    {subTableElement}
                                </TableBody>
                            </Table>
                        </Box>
                    </Collapse>
                </TableCell>
            </TableRow>
        )
    }

    // const setTable = (table: AbstractTable, nestedObject: any[]) => {
    //     const nestTables: JSX.Element[] = [];
    //     if(table.subTables){
    //         const subTableName = table.subTables.name;
    //         const subTableColumnNames = table.subTables.columnNames;
    //
    //         const collapsable = setTable(table.subTables, nestedObject[subTableName]);
    //         return <CollapsableTable collapsableTableElement={collapsable} rows={rows} columnsHeading={table.columnNames}/>
    //     }else{
    //             return rows.map((nestedObject: { [x: string]: any; }) => {
    //                const singleData = nestedObject[table.name];
    //                 console.log("[data]", singleData)
    //                return (<NewRow data={nestedObject} table={table}/>)
    //                 })
    //     }
    //
    // }

    const element = props.tableHeaders ? props.tableHeaders.columnNames.map((e) => <TableCell key={e.concat(Math.random().toString())}>{e}</TableCell>):null;
    const body = props.tableHeaders ? rows.map((row) => {
            return processTable(props.tableHeaders!, row)
        }):null;
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
