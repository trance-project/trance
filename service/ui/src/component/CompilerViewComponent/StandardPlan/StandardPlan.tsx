/**
 * StandardPlan used to generate a tree diagram of the illustration and the Plan out ofter a query has been compiled
 * and executed
 */

import React from "react";
import Tree from "react-d3-tree";
import {CustomNodeElementProps} from "react-d3-tree/lib/types/common";
import {Grid} from "@material-ui/core";

import projection from '../../../static/images/planOperator/Projection.png';
import nest from '../../../static/images/planOperator/Nest.png';
import sumAggregate from '../../../static/images/planOperator/Sum_aggregate.png';
import leftOuterJoin from '../../../static/images/planOperator/LeftOuterJoin.png';
import outerUnnest from '../../../static/images/planOperator/outer-unest.png';
import unnest from "../../../static/images/planOperator/Unnest.png";
import selection from "../../../static/images/planOperator/Selection.png";
import {useAppDispatch, useAppSelector} from '../../../redux/Hooks/hooks';
import ForwardIcon from "@material-ui/icons/Forward";
import {runStandardPlan} from '../../../redux/QuerySlice/thunkQueryApiCalls';
import Button from "@material-ui/core/Button";


// Here we're using `renderCustomNodeElement` to bind event handlers
// to the DOM nodes of our choice.
// In this case, we only want the node to toggle if the *label* is clicked.
// Additionally we've replaced the circle's `onClick` with a custom event,
// which differentiates between branch and leaf nodes.
const renderNodeWithCustomEvents = (diagramElProps: CustomNodeElementProps) => {
    const level = parseInt(diagramElProps.nodeDatum.attributes?.level!);
    const levelColor = _colorPicker(level);
    // @ts-ignore
    const newLine = diagramElProps.nodeDatum.attributes?.newLine.join(",");
    return (
        <g>
            <circle r="15" onClick={diagramElProps.toggleNode} fill={levelColor}/>
            <foreignObject width={70} height={50}>
                {getImagePlanOperator(diagramElProps.nodeDatum.attributes?.planOperator)}
            </foreignObject>
            <text fill="black" strokeWidth="0.5" x="50" y={"0"} fontSize={28} onClick={diagramElProps.toggleNode}>
                {diagramElProps.nodeDatum.attributes?.planOperator}
            </text>
            <text fill="black" strokeWidth="0.5" x="50" y={"15"} fontSize={25} onClick={diagramElProps.toggleNode}>
                {diagramElProps.nodeDatum.name}
            </text>
            {diagramElProps.nodeDatum.attributes?.newLine && (
                <text fill="black" x="50" dy="30" fontSize={20} strokeWidth="0.5">
                    {newLine}
                </text>
            )}
            {level >= 0 && (
                <text fill="black" x="50" fontSize={18} dy={diagramElProps.nodeDatum.attributes?.newLine?"50":"40"} strokeWidth="1">
                    level: {diagramElProps.nodeDatum.attributes?.level}
                </text>
            )}
        </g>
    );
}

const getImagePlanOperator = (planOperator:string | number | boolean | undefined) => {

    let image;
    switch (planOperator){
        case 'PROJECT':
            image=projection;
            break;
        case 'SELECT':
            image=selection;
            break;
        case 'NEST':
            image=nest;
            break;
        case 'SUM':
            image=sumAggregate;
            break;
        case 'OUTERJOIN':
            image=leftOuterJoin;
            break;
        case 'OUTERUNNEST':
            image=outerUnnest;
            break;
        case 'UNNEST':
            image=unnest;
            break;
        default:
            return;
    }
    return <img src={image} alt={'planOperatorSymbol'} width={30} height={30}/>
}

const _colorPicker =(level:number)=>{
    switch (level) {
        case 0:
            return "rgba(141,158,145,0.8)"; //"#8D9E91";
        case 1:
            return "rgba(0, 140, 212, 0.7)"; //"rgba(139,122,140,0.3)"; //"#8B7A8C";
        case 2:
            return "rgba(179,130,181,0.8)"; //"#B382B5";
        case 3:
            return "rgba(243,71,246,0.8)";
        case 4:
            return "rgba(5,93,165,0.8)";
    }
}

const StandardPlan = () => {
    const dispatch = useAppDispatch();

    const treeDiagramData = useAppSelector(state => state.query.standardPlan);
    const selectedQuery = useAppSelector(state => state.query.selectedQuery);
    const nrcCode = useAppSelector(state => state.query.nrcQuery);

    const handleButtonClick = () => {
        dispatch(runStandardPlan({
            _id: selectedQuery!._id,
            body: nrcCode,
            title: selectedQuery!.name
        }))
    }
    return (
        <div>
            <Grid container direction={"row"} style={{height: "675px"}}>
                <Grid item xs={12} style={{height: '900px'}}>
                        <Tree
                            data={treeDiagramData}
                            orientation={"vertical"}
                            pathFunc={"straight"}
                            zoom={0.65}
                            enableLegacyTransitions
                            translate={{x:400, y:20}}
                            transitionDuration={1500}
                            renderCustomNodeElement={diagramProps => renderNodeWithCustomEvents(diagramProps)}
                            separation={{siblings:2.25}}
                            zoomable={true}
                        />
                </Grid>
                <Button variant={"contained"} color={"primary"} onClick={handleButtonClick} endIcon={<ForwardIcon/>}>Run</Button>
            </Grid>

        </div>
    )
}

export default StandardPlan;


