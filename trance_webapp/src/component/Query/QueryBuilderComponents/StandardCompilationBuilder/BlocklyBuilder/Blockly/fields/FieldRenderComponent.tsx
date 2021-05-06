import React from 'react';
import Autocomplete from "@material-ui/lab/Autocomplete";
import TextField from "@material-ui/core/TextField";
import {TempTable} from '../../../../../../../utils/Public_Interfaces'

import {useAppSelector} from '../../../../../../../redux/Hooks/hooks';

interface _FieldRenderComponentProps {
    onSelect : (event: {name:string, table:string } | null) => void;
}

const FieldRenderComponent = (props: _FieldRenderComponentProps) => {

    // const objects = useAppSelector(state => state.tranceObject.objects);
    // Top 100 films as rated by IMDb users. http://www.imdb.com/chart/top

    const objects : TempTable[] = [{
        _id:'de2f4d6e-1e31-4731-bb6b-91cab17ab5ff',
        name: "Sample",
        abr: 's',
        columns: [
            {name: 'sample' },
            {name: 'turmorsite' },
            {name: 'treatment_outcome' },
        ]
    },
        {
            _id:'efc73e9a-99d0-43f2-b0fe-8dc7827df592',
            name: "CopyNumber",
            abr: 'c',
            columns: [
                {name: 'sample' },
                {name: 'gene' },
                {name: 'cnum' },
            ]
        },
        {
            _id:'503f6711-69be-4a60-8de2-1aa8f017dd6e',
            name: "Occurrences",
            abr: 'o',
            columns: [
                {name: 'sample' },
                {name: 'contig' },
                {name: 'end' },
                {name: 'reference' },
                {name: 'alternate' },
                {name: 'mutid' },
                {name: 'candidates' },
            ]
        }]

    const options = objects.map((option) => {
        return option.columns.map(c => {
            return{
                table: option.name,
                ...c,
            }
        });
    }).reduce((previousValue, currentValue) => previousValue.concat(currentValue), []);
    return (
        <Autocomplete
            id="grouped-demo"
            options={options.sort((a, b) => -b.table.localeCompare(a.table))}
            groupBy={(option) => option.table}
            getOptionLabel={(option) => option.name}
            style={{ width: 300 }}
            renderInput={(params) => <TextField {...params} label="With categories" variant="outlined" />}
            onChange={(event,value: {name:string, table:string } | null, reason: string) => props.onSelect(value)}
        />
    )
}

export default FieldRenderComponent;