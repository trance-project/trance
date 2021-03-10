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
}

const ChipSelect = (props:_ChipSelectProps) => {
    const classes = chipSelect();
    const theme = useTheme();
    const [personName, setPersonName] = React.useState<string[]>([]);

    const handleChange = (event: React.ChangeEvent<{ value: unknown }>) => {
        setPersonName(event.target.value as string[]);
    };

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

    const handleChangeMultiple = (event: React.ChangeEvent<{ value: unknown }>) => {
        const { options } = event.target as HTMLSelectElement;
        const value: string[] = [];
        for (let i = 0, l = options.length; i < l; i += 1) {
            if (options[i].selected) {
                value.push(options[i].value);
            }
        }
        setPersonName(value);
    };
    return(
        <React.Fragment>
            <FormControl className={classes.formControl}>
                <InputLabel id="demo-mutiple-chip-label">{props.table.name}</InputLabel>
                <Select
                    labelId="demo-mutiple-chip-label"
                    id="demo-mutiple-chip"
                    multiple
                    value={personName}
                    onChange={handleChange}
                    input={<Input id="select-multiple-chip" />}
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
                                  style={getStyles(column.name, personName, theme)}>
                            {column.name}
                        </MenuItem>
                    ))}
                </Select>
            </FormControl>
        </React.Fragment>
    )
}


export default ChipSelect;