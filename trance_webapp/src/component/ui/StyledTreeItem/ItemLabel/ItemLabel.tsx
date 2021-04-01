import React from 'react';
import Typography from '@material-ui/core/Typography';
import Checkbox from '@material-ui/core/Checkbox';

import itemLabelThemeStyle from "./ItemLabelThemeStyle";

interface _ItemLabelProps{
       labelText: string;
       checkBoxClicked: () => void;
       checkBoxEnabled?: boolean;
       isColumn?: boolean;
}

const ItemLabel = (props: _ItemLabelProps) => {
        const classes = itemLabelThemeStyle();
        const checkBox = (props.isColumn ? <Checkbox
                checked={props.checkBoxEnabled}
                onChange={props.checkBoxClicked}
                inputProps={{ 'aria-label': 'primary checkbox' }}
                />: null);
        return (
            <div className={classes.labelRoot}>
                    <Typography variant="body2" className={classes.labelText}>
                            {props.labelText}
                    </Typography>
                {checkBox}
            </div>
        );
}

export default ItemLabel