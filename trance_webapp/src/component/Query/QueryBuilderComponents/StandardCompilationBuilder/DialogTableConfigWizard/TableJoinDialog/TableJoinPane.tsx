import React from 'react';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import PaperWithHeader from "../../../../../ui/Paper/PaperWithHeader/PaperWithHeader";
import JoinConfig from "../../../QueryConfig/TableConfig/JoinConfig/JoinConfig";

import {tableJoinDialogThemeStyle} from './TableJoinDialogThemeStyle'
import {Association} from "../../../../../../utils/Public_Interfaces";

interface _TableJoinDialogProps {
    open:boolean;
    close:()=>void;

    associations: Association[];
    objects: string[];
    onAssociation: (objectAssociations:string[]) => void;
    onAssociationDelete: (key:number)=>void;
}

const TableJoinPane = (props:_TableJoinDialogProps) => {
    const classes = tableJoinDialogThemeStyle();

    return (
        <div className={classes.rootEL}>
            <Dialog open={props.open} onClose={props.close} aria-labelledby="form-dialog-title" maxWidth={"lg"} fullWidth>
                <DialogTitle id="form-dialog-title">Association Object</DialogTitle>
                <DialogContent>
                    <PaperWithHeader heading={'Association'} height={450}>
                        <JoinConfig
                            tables={props.objects}
                            associations={props.associations}
                            onAssociation={props.onAssociation}
                            onAssociationDelete={props.onAssociationDelete}
                        />
                    </PaperWithHeader>
                </DialogContent>
                <DialogActions>
                    <Button onClick={props.close} color="primary" variant={"outlined"}>
                        Cancel
                    </Button>
                    <Button onClick={props.close} color="primary" variant={"contained"}>
                        Done
                    </Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}

export default TableJoinPane;