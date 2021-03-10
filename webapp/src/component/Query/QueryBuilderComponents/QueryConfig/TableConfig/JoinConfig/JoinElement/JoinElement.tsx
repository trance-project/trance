import React from 'react';
import { createStyles, makeStyles, Theme } from '@material-ui/core/styles';
import Chip from '@material-ui/core/Chip';
import Paper from '@material-ui/core/Paper';
import TagFacesIcon from '@material-ui/icons/TagFaces';

import {joinElementThemeStyle} from './JoinElementThemeStyle';

interface ChipData {
    key: number;
    label: string;
}

const JoinElement = ()  => {
    const classes = joinElementThemeStyle();
    const [chipData, setChipData] = React.useState<ChipData[]>([
        { key: 0, label: 'Sample.sample' },
        { key: 1, label: 'Occurrences.sample' },
    ]);

    const handleDelete = (chipToDelete: ChipData) => () => {
        setChipData((chips) => chips.filter((chip) => chip.key !== chipToDelete.key));
    };

    return (
        <Paper component="ul" className={classes.root}>
            {chipData.map((data) => {
                let icon;

                if (data.label === 'React') {
                    icon = <TagFacesIcon />;
                }

                return (
                    <li key={data.key}>
                        <Chip
                            icon={icon}
                            label={data.label}
                            onDelete={data.label === 'React' ? undefined : handleDelete(data)}
                            className={classes.chip}
                        />
                    </li>
                );
            })}
        </Paper>
    );
}

export default JoinElement;