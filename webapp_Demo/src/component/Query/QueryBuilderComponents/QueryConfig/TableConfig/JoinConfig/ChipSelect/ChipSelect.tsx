import React from "react";
import {Typography} from "@material-ui/core";
import {useTheme, Theme} from "@material-ui/core/styles";
import Input from '@material-ui/core/Input';
import InputLabel from '@material-ui/core/InputLabel';
import MenuItem from '@material-ui/core/MenuItem';
import FormControl from '@material-ui/core/FormControl';
import Select from '@material-ui/core/Select';
import Chip from '@material-ui/core/Chip';
import ListSubheader from '@material-ui/core/ListSubheader';
import {Table} from '../../../../../../../Interface/Public_Interfaces';

import {chipSelect} from './ChipSelectThemeStyle';


const ITEM_HEIGHT = 48;
const ITEM_PADDING_TOP = 8;
const MenuProps = {
    PaperProps: {
        style: {
            maxHeight: ITEM_HEIGHT * 4.5 + ITEM_PADDING_TOP,
            width: 250,
        },
    },
};

function getStyles(name: string, personName: string[], theme: Theme) {
    return {
        fontWeight:
            personName.indexOf(name) === -1
                ? theme.typography.fontWeightRegular
                : theme.typography.fontWeightMedium,
    };
}
interface _ChipSelectProps{
    table:Table;
    columns:string[];
    setColumns:(event: React.ChangeEvent<{ value: unknown }>)=>void;
}

const ChipSelect = (props:_ChipSelectProps) => {
    const classes = chipSelect();
    const theme = useTheme();

    const colorStyle = (index:number) =>{
        switch (index){
            case 0:
                return '#2ecc71'
            case 1:
                return '#2980b9'
            case 2:
                return '#e67e22'
            default:
                return ;
        }

    }
    return(
        <React.Fragment>
            <FormControl className={classes.formControl}>
                <InputLabel id={"demo-mutiple-chip-label"+props.table.name} >{props.table.name}</InputLabel>
                <Select
                    labelId={"demo-mutiple-chip-label"+props.table.name}
                    id={"demo-mutiple-chip"+props.table.name}
                    multiple
                    value={props.columns}
                    onChange={props.setColumns}
                    input={<Input id={"select-multiple-chip"+props.table.name} />}
                    renderValue={(selected) => (
                        <div className={classes.chips}>
                            {(selected as string[]).map((value, i) =>
                               <Chip key={value} label={value} className={classes.chip}
                                     style={{backgroundColor: colorStyle(i)}}/>
                            )}
                        </div>
                    )}
                    MenuProps={MenuProps}
                >
                    {props.table.columns.map(column => (
                        <MenuItem key={column.id} value={column.name}
                                  style={getStyles(column.name, props.columns, theme)}>
                            {column.name}
                        </MenuItem>
                    ))}
                </Select>
            </FormControl>
        </React.Fragment>
    )
}


export default ChipSelect;