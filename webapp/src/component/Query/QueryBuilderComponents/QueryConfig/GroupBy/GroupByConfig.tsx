import React from "react";
import {Typography} from "@material-ui/core";
import Grid from '@material-ui/core/Grid';
import List from '@material-ui/core/List';
import Card from '@material-ui/core/Card';
import CardHeader from '@material-ui/core/CardHeader';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import Checkbox from '@material-ui/core/Checkbox';
import Button from '@material-ui/core/Button';
import Divider from '@material-ui/core/Divider';

import {groupByConfigThemeStyle} from './GroupByConfigThemeStyle';

function not(a: GroupByAttributes[], b: GroupByAttributes[]) {
    return a.filter((value) => b.indexOf(value) === -1);
}

function intersection(a: GroupByAttributes[], b: GroupByAttributes[]) {
    return a.filter((value) => b.indexOf(value) !== -1);
}

function union(a: GroupByAttributes[], b: GroupByAttributes[]) {
    return [...a, ...not(b, a)];
}

interface GroupByAttributes{
    id:number;
    label:string;
}

const initGroupDetail:GroupByAttributes[] = [
    {id:0,label:'Gene'},
    {id:1,label:'Sample'},
    {id:2,label:'Score'},
    {id:3,label:'Mutid'},
]

const GroupByConfig = () => {
    const classes = groupByConfigThemeStyle();

    const [checked, setChecked] = React.useState<GroupByAttributes[]>([]);
    const [left, setLeft] = React.useState<GroupByAttributes[]>(initGroupDetail);
    const [right, setRight] = React.useState<GroupByAttributes[]>([]);

    const leftChecked = intersection(checked, left);
    const rightChecked = intersection(checked, right);

    const handleToggle = (value: GroupByAttributes) => () => {
        const currentIndex = checked.indexOf(value);
        const newChecked = [...checked];

        if (currentIndex === -1) {
            newChecked.push(value);
        } else {
            newChecked.splice(currentIndex, 1);
        }

        setChecked(newChecked);
    };

    const numberOfChecked = (items: GroupByAttributes[]) => intersection(checked, items).length;

    const handleToggleAll = (items: GroupByAttributes[]) => () => {
        if (numberOfChecked(items) === items.length) {
            setChecked(not(checked, items));
        } else {
            setChecked(union(checked, items));
        }
    };

    const handleCheckedRight = () => {
        setRight(right.concat(leftChecked));
        setLeft(not(left, leftChecked));
        setChecked(not(checked, leftChecked));
    };

    const handleCheckedLeft = () => {
        setLeft(left.concat(rightChecked));
        setRight(not(right, rightChecked));
        setChecked(not(checked, rightChecked));
    };

    const customList = (title: React.ReactNode, items: GroupByAttributes[]) => (
        <Card>
            <CardHeader
                className={classes.cardHeader}
                avatar={
                    <Checkbox
                        onClick={handleToggleAll(items)}
                        checked={numberOfChecked(items) === items.length && items.length !== 0}
                        indeterminate={numberOfChecked(items) !== items.length && numberOfChecked(items) !== 0}
                        disabled={items.length === 0}
                        inputProps={{ 'aria-label': 'all items selected' }}
                    />
                }
                title={title}
                subheader={`${numberOfChecked(items)}/${items.length} selected`}
            />
            <Divider />
            <List className={classes.list} dense component="div" role="list">
                {items.map((value: GroupByAttributes) => {
                    const labelId = `transfer-list-all-item-${value.id}-label`;

                    return (
                        <ListItem key={value.id} role="listitem" button onClick={handleToggle(value)}>
                            <ListItemIcon>
                                <Checkbox
                                    checked={checked.indexOf(value) !== -1}
                                    tabIndex={-1}
                                    disableRipple
                                    inputProps={{ 'aria-labelledby': labelId }}
                                />
                            </ListItemIcon>
                            <ListItemText id={labelId} primary={value.label} />
                        </ListItem>
                    );
                })}
                <ListItem />
            </List>
        </Card>
    );
    return(
        <React.Fragment>
        <Typography>Please select the nested attributes to be Grouped By</Typography>
            <Grid container spacing={2} justify="center" alignItems="center" className={classes.root}>
                <Grid item>{customList('Select Attributes', left)}</Grid>
                <Grid item>
                    <Grid container direction="column" alignItems="center">
                        <Button
                            variant="outlined"
                            size="small"
                            className={classes.button}
                            onClick={handleCheckedRight}
                            disabled={leftChecked.length === 0}
                            aria-label="move selected right"
                        >
                            &gt;
                        </Button>
                        <Button
                            variant="outlined"
                            size="small"
                            className={classes.button}
                            onClick={handleCheckedLeft}
                            disabled={rightChecked.length === 0}
                            aria-label="move selected left"
                        >
                            &lt;
                        </Button>
                        <Button
                            variant="contained"
                            size="small"
                            className={classes.button}
                            onClick={handleCheckedLeft}
                            // disabled={right.length > 0}
                            aria-label="Group right selected"
                        >
                           Group
                        </Button>
                    </Grid>
                </Grid>
                <Grid item>{customList('Chosen Attributes', right)}</Grid>
            </Grid>
        </React.Fragment>
    )
}

export default GroupByConfig;