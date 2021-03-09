import React from "react";
import Grid from "@material-ui/core/Grid";
import Paper from "@material-ui/core/Paper";

import {ListItem, ListItemText, Typography,Divider} from "@material-ui/core";
import {Table} from "../../Interface/Public_Interfaces";
import { FixedSizeList, ListChildComponentProps } from 'react-window';
import {viewSelectorThemeStyle} from './ViewSelectorThemeStyle';

interface _ViewSelectorProps{
    gridXS: boolean | "auto" | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | undefined;
    gridMD: boolean | "auto" | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | undefined;
    gridLG: boolean | "auto" | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | undefined;
    style: {};
    tables: Table[];
    clicked: (table:Table) => void;

}

const ViewSelector = (props:_ViewSelectorProps) => {
    const classes = viewSelectorThemeStyle();
    const itemCount = props.tables.length;
    const tables = props.tables;
    const [checked, setChecked] = React.useState([1]);

    const handleToggle = (value: number) => () => {
        const currentIndex = checked.indexOf(value);
        const newChecked = [...checked];

        if (currentIndex === -1) {
            newChecked.push(value);
        } else {
            newChecked.splice(currentIndex, 1);
        }

        setChecked(newChecked);
    };

    const tableCard = (
        props.tables.map((el,index) => {
            return (
                <React.Fragment>
                    <ListItem button key={index + Math.random().toString()}>
                       <ListItemText primary={el.name}/>
                    </ListItem>
                    <Divider />
                </React.Fragment>
            )
        })
    );

    function renderRow(rowProps: ListChildComponentProps) {
        const { index, style } = rowProps;
        const labelId = `checkbox-list-secondary-label-${index}`;
        return (
            <ListItem button style={style} key={index}>
                <ListItemText id={labelId} primary={tables[index].name} onClick={() => props.clicked(tables[index])}/>
            </ListItem>
        );
    }

    return (
        <Grid item xs={props.gridXS} md={props.gridMD} lg={props.gridLG}>
            <Paper style={props.style} className={classes.root}>
                <Typography variant={"h5"}>Inputs</Typography>
                <FixedSizeList height={400} width={300} itemSize={46} itemCount={itemCount}>
                        {renderRow}
                </FixedSizeList>
            </Paper>
        </Grid>
    );
}

export default ViewSelector;