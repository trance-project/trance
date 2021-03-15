import React, {forwardRef, ForwardedRef} from "react";
import {Typography} from "@material-ui/core";
import {useTheme} from "@material-ui/core/styles";
import Fab from '@material-ui/core/Fab';
import AddIcon from '@material-ui/icons/Add';
import VerticalAlignCenterIcon from '@material-ui/icons/VerticalAlignCenter';
import GroupWorkIcon from '@material-ui/icons/GroupWork';
import Zoom from '@material-ui/core/Zoom';


import {labelViewThemeStyle} from './LabelViewThemeStyle';

interface _LabelViewProps {
    tableName: string;
    tableEl: string;
    selectNode?: ()=>void;
    joinString?: string;
    columns?: String[];
    sumBy?: boolean;
    hoverEvent?: () => void;
    abortHover?:()=>void;
    isSelected?:boolean;

}

const LabelView = forwardRef((props:_LabelViewProps, ref:ForwardedRef<any>) => {
    const classes = labelViewThemeStyle();
    const theme = useTheme();
    const sumByEl = props.sumBy? <Typography variant={"body1"}> sumBy<Typography component={"span"} style={{fontSize:'10px'}}>score gene(</Typography></Typography> : null;
    const actionButtonStyle=props.isSelected?classes.actionButton:classes.actionDisableButton;

    const transitionDuration = {
        enter: theme.transitions.duration.enteringScreen,
        exit: theme.transitions.duration.leavingScreen,
    };
    return(
         <div className={classes.container} ref={ref} onClick={props.selectNode}>
            <div className={classes.root} onMouseEnter={props.hoverEvent} onMouseLeave={props.abortHover}>
                {sumByEl}
                <Typography variant={"body1"}><Typography component={"span"}>for</Typography> {props.tableEl} <Typography component={"span"}> in </Typography> {props.tableName} <Typography component={"span"}> union </Typography></Typography>

                {props.joinString?<Typography variant={"body1"}><Typography component={"span"}>if</Typography> {props.joinString} <Typography component={"span"}>then</Typography> </Typography>:null}

                {props.columns?<Typography variant={"body1"}>{"{("} {props.columns.join(" , ")}</Typography>:null}
            </div>
             <Zoom
                 in={props.isSelected}
                 timeout={transitionDuration}
                 style={{
                     transitionDelay: `${transitionDuration ? transitionDuration.exit : 0}ms`,
                 }}
                 unmountOnExit>
                    <div className={actionButtonStyle}>

                            <Fab size={"small"} color="primary" aria-label="add" >
                                <AddIcon />
                            </Fab>
                            <Fab size={"small"}  color="primary" aria-label="edit">
                                <GroupWorkIcon />
                            </Fab>
                            <Fab size={"small"}  color="primary">
                                <VerticalAlignCenterIcon/>
                            </Fab>
                    </div>
             </Zoom>
         </div>
    );
});

export default LabelView;
