import React from "react";
import TableRow from "@material-ui/core/TableRow";
import TableCell from "@material-ui/core/TableCell";
import IconButton from "@material-ui/core/IconButton";
import KeyboardArrowUpIcon from "@material-ui/icons/KeyboardArrowUp";
import KeyboardArrowDownIcon from "@material-ui/icons/KeyboardArrowDown";

import {rowThemeStyle} from './RowThemeStyle';
import {AbstractTable} from "../../../utils/Public_Interfaces";
import CollapsableTable from "./CollapsableTable";

interface _RowProps {
    table: AbstractTable;
    nestedObject: any;

}

const Row = (props: _RowProps) => {
    const classes = rowThemeStyle();
    const [open, setOpen] = React.useState(false);

    const elementCollapseRow: JSX.Element[] = [];
    const elementColumn: JSX.Element[] = [];
    let hasNestedElement = false;
    let collapsable = false;
    for( let [key] of Object.entries(props.nestedObject)){
        if(key === props.table.subTables?.name){
            hasNestedElement = props.nestedObject[key].length>0;
            collapsable = true
            elementCollapseRow.push(<CollapsableTable table={props.table.subTables} show={open} object={props.nestedObject[key]}/>);
        }else{
            elementColumn.push((
                <TableCell>
                    {props.nestedObject[key]}
                </TableCell>
            ))
        }
    }

    const styleExpand = hasNestedElement?classes.expand:classes.noExpand;

    return(
        <React.Fragment>
            <TableRow>
                {collapsable?
                    <TableCell className={classes.tableCell}>
                        <IconButton aria-label="expand row" size="small" onClick={() => setOpen(!open)}
                                    className={styleExpand}>
                            {open ? <KeyboardArrowUpIcon/> : <KeyboardArrowDownIcon/>}
                        </IconButton>
                    </TableCell>:null}
                {elementColumn}
            </TableRow>
            {elementCollapseRow}
        </React.Fragment>
    )
}

export default Row