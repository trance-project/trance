import React from 'react';
import Autocomplete from "@material-ui/lab/Autocomplete";
import TextField from "@material-ui/core/TextField";
import {TempTable} from '../../../../../../../utils/Public_Interfaces'

import {useAppSelector} from '../../../../../../../redux/Hooks/hooks';

interface _FieldRenderComponentProps {
    onSelect : any;
}

const CollectionFieldRenderComponent = (props: _FieldRenderComponentProps) => {


    const objects = useAppSelector(state => state.tranceObject.objects);


    const options = objects.map((option) => {
        return option.columns.map(c => {
            if(c.dataType==="Collection")
                return{
                table: option.name,
                name: `${option.abr}.${c.name}`,
            }
        }).filter(Boolean);
    }).reduce((previousValue, currentValue) => previousValue.concat(currentValue), []);
    return (
        <Autocomplete
            id="grouped-demo"
            options={options.sort((a, b) => -b!.table.localeCompare(a!.table))}
            groupBy={(option) => option!.table}
            getOptionLabel={(option) => option!.name}
            style={{ width: 300 }}
            renderInput={(params) => <TextField {...params} label="With categories" variant="outlined" />}
            onChange={(event,value: {name:string, table:string } | null | undefined, reason: string) => props.onSelect(value)}
        />
    )
}

export default CollectionFieldRenderComponent;