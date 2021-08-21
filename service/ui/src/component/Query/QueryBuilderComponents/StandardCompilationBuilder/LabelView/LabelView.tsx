import React, {forwardRef, ForwardedRef} from "react";
import {Typography} from "@material-ui/core";
import {useTheme} from "@material-ui/core/styles";
import Fab from '@material-ui/core/Fab';
import GroupWorkIcon from '@material-ui/icons/GroupWork';
import ArrowBackIcon from '@material-ui/icons/ArrowBack';
import ArrowDownwardIcon from '@material-ui/icons/ArrowDownward';
import Zoom from '@material-ui/core/Zoom';


import {labelViewThemeStyle} from './LabelViewThemeStyle';
import {GroupBy, Table, LabelAssociation} from '../../../../../utils/Public_Interfaces';



interface _LabelViewProps {
    tableName: string;
    tableEl: string;
    nestedObjectJoin?:string;
    nestedObjectEl?:string;
    selectNode?: ()=>void;
    association?: LabelAssociation;
    columns?: String[];
    groupBy?: GroupBy;
    hoverEvent?: () => void;
    abortHover?:()=>void;
    isSelected?:boolean;
    openJoinAction?: () => void;
    openEdit?: () => void;
    openGroupBy?: () => void;
    endScope?: string;

}

const LabelView = forwardRef((props:_LabelViewProps, ref:ForwardedRef<any>) => {
    const classes = labelViewThemeStyle();
    const theme = useTheme();
    const sumByEl = props.groupBy? <Typography variant={"body1"}> ${props.groupBy.type}<Typography component={"span"} style={{fontSize:'10px'}}>{props.groupBy.key}</Typography>(</Typography> : null;
    const actionButtonStyle=props.isSelected?classes.actionButton:classes.actionDisableButton;

    const joinString = props.association?matchAssociation(props.association):null;

    const transitionDuration = {
        enter: theme.transitions.duration.enteringScreen,
        exit: theme.transitions.duration.leavingScreen,
    };

    const endScope = props.groupBy?")}))})}":props.endScope;
    return(
         <div className={classes.container} ref={ref} onClick={props.selectNode}>
            <div className={classes.root} onMouseEnter={props.hoverEvent} onMouseLeave={props.abortHover}>
                {sumByEl}
                {joinString}
                <Typography variant={"body1"}><Typography component={"span"}>for</Typography> {props.tableEl} <Typography component={"span"}> in </Typography> {props.tableName} <Typography component={"span"}> union </Typography></Typography>
                {props.association?<Typography variant={"body1"}><Typography component={"span"}>if</Typography> {props.association.join} <Typography component={"span"}>then</Typography> </Typography>:null}

                {props.columns?<Typography variant={"body1"}>{'{('}{props.columns.join(" , ")}{endScope}</Typography>:null}
            </div>
             <Zoom
                 in={props.isSelected}
                 timeout={transitionDuration}
                 style={{
                     transitionDelay: `${transitionDuration ? transitionDuration.exit : 0}ms`,
                 }}
                 unmountOnExit>
                    <div className={actionButtonStyle}>
                        <Fab size={"small"} color="primary" aria-label="add" onClick={props.openEdit}>
                            <ArrowBackIcon />
                        </Fab>
                        <Fab size={"small"}  color="primary" onClick={props.openJoinAction}>
                            <ArrowDownwardIcon/>
                        </Fab>
                        <Fab size={"small"}  color="primary" aria-label="edit" onClick={props.openGroupBy}>
                            <GroupWorkIcon />
                        </Fab>
                    </div>
             </Zoom>
         </div>
    );
});


const matchAssociation = (association: LabelAssociation) => {
    const jsxElement: JSX.Element[] = [];
    if(association.tables){
        association.tables.forEach(t => {
            jsxElement.push(...filterObject(t));
        });
    }
    return jsxElement;
}

const filterObject = (table: Table) => {
    const jsxElement: JSX.Element[] = [];
    table.columns.forEach(column => {
        if(column.children){
            jsxElement.push(...filterObject(column.children));
        }else{
            jsxElement.push(
                <div key={column.tableAssociation+column.name}>
                <Typography variant={"body1"}><Typography component={"span"}>for</Typography> {table.abr}
                <Typography component={"span"}> in </Typography> {table.name} <Typography
                    component={"span"}> union </Typography></Typography>
                </div>
            )
        }
    })
    return jsxElement;
}

export default LabelView;
