import React from "react";

import {rowThemeStyle} from './RowThemeStyle';
import TableRow from "@material-ui/core/TableRow";
import TableCell from "@material-ui/core/TableCell";
import IconButton from "@material-ui/core/IconButton";
import KeyboardArrowUpIcon from "@material-ui/icons/KeyboardArrowUp";
import KeyboardArrowDownIcon from "@material-ui/icons/KeyboardArrowDown";

interface _RowProps {
    elementColumn: JSX.Element[];
    elementCollapseRow: JSX.Element[];
    elementNested?: Boolean;
    elementCollapsable?: Boolean

}

const Row = (props: _RowProps) => {
    const classes = rowThemeStyle();
    const [open, setOpen] = React.useState(false);
    const styleExpand = props.elementNested?classes.expand:classes.noExpand;

    return(
        <React.Fragment>
            <TableRow>
                {props.elementCollapsable?
                    <TableCell className={classes.tableCell}>
                        <IconButton aria-label="expand row" size="small" onClick={() => setOpen(!open)}
                                    className={styleExpand}>
                            {open ? <KeyboardArrowUpIcon/> : <KeyboardArrowDownIcon/>}
                        </IconButton>
                    </TableCell>:null}
                {props.elementColumn}
            </TableRow>
            {props.elementCollapseRow}
        </React.Fragment>
    )
}

export default Row