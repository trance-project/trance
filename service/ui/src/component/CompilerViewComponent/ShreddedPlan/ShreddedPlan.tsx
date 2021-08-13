import React from "react";
import {Grid} from "@material-ui/core";
import Tree from "react-d3-tree";
import {CustomNodeElementProps, RawNodeDatum} from "react-d3-tree/lib/types/common";

import projection from "../../../static/images/planOperator/Projection.png";
import nest from "../../../static/images/planOperator/Nest.png";
import sumAggregate from "../../../static/images/planOperator/Sum_aggregate.png";
import leftOuterJoin from "../../../static/images/planOperator/LeftOuterJoin.png";
import unnest from "../../../static/images/planOperator/Unnest.png";
import equiJoin from "../../../static/images/planOperator/Equijoin.png";
import {useAppSelector,useAppDispatch} from '../../../redux/Hooks/hooks';
import {runStandardPlan} from '../../../redux/QuerySlice/thunkQueryApiCalls';
import Button from "@material-ui/core/Button";
import ForwardIcon from "@material-ui/icons/Forward";

const ShreddedPlan = () => {

    const dispatch = useAppDispatch();

    const shreddedResponse = useAppSelector(state => state.query.shreddedResponse);
    const selectedQuery = useAppSelector(state => state.query.selectedQuery);
    const nrcCode = useAppSelector(state => state.query.nrcQuery);

    let planElement: JSX.Element[] = [];

    const handleButtonClick = () => {
        dispatch(runStandardPlan({
            _id: selectedQuery!._id,
                body: nrcCode,
                title: selectedQuery!.name
        }))
    }
    if(shreddedResponse){
      planElement = shreddedResponse.shred_plan.map((el,index) => (
          <Grid item key={index} xs={12}>
            <Tree
                data={el}
                orientation={"vertical"}
                pathFunc={"straight"}
                zoom={0.6}
                enableLegacyTransitions
                translate={{x: 400, y: 20}}
                transitionDuration={1500}
                renderCustomNodeElement={diagramProps => renderNodeWithCustomEvents(diagramProps)}
                zoomable={false}
            />
          </Grid>
        ))
    }
    return (
        <Grid container direction={"row"}>
            {planElement}
            <Button variant={"contained"} color={"primary"} onClick={handleButtonClick} endIcon={<ForwardIcon/>}>Run</Button>
        </Grid>
    );
}

const treeDiagramLvl1:RawNodeDatum = {
    name: '',
    attributes: {
        newLine: 'sample,mutations:=Newlabel(sample)',
        level:'1',
        planOperator:'PROJECT'
    },
    children:[
        {
            name: 'MatSamples',
            attributes: {
                level: '1'
            },
        }
    ]
}
const treeDiagramLvl2:RawNodeDatum = {
    name: '',
    attributes: {
        newLine: 'mutId, scores:= NewLabel(sample)',
        level:'2',
        planOperator:'PROJECT'
    },
    children:[
        {
            name: 'MatOccurences',
            attributes: {
                level: '2'
            },
        }
    ]
}
const treeDiagramData:RawNodeDatum = {
    name:'',
    attributes:{
        newLine: 'label,gene,score',
        level:'3',
        planOperator:'PROJECT'
    },
    children:[{
        name: '',
        attributes:{
            //newLine: 'impact*(cnum+0.01)*sift*poly',
            newLine: 'sample, label, gene',
            level:'3',
            planOperator:'SUM'
        },
        children:[{
            name:'',
            attributes:{
                newLine: 'sample, gene',
                level:'3',
                planOperator:'JOIN'
            },
            children:[
                {
                    name:'',
                    attributes:{
                        newLine: 'label',
                        level:'3',
                        planOperator:'JOIN'
                    },
                    children:[
                        {
                            name:'LabelDomain', //_mutations_scores',
                            attributes:{
                                level:'3'
                            }
                        },
                        {
                            name:'MatOccurences_mutations',
                            attributes:{
                                level:'3'
                            }
                        }
                    ]
                },
                {
                    name:'CopyNumber',
                    attributes:{
                        level:'3'
                    }
                }
            ]
        }]
    }]
}


// Here we're using `renderCustomNodeElement` to bind event handlers
// to the DOM nodes of our choice.
// In this case, we only want the node to toggle if the *label* is clicked.
// Additionally we've replaced the circle's `onClick` with a custom event,
// which differentiates between branch and leaf nodes.
const renderNodeWithCustomEvents = (diagramElProps: CustomNodeElementProps) => {
    const levelColor = _colorPicker(diagramElProps.nodeDatum.attributes?.level!);
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
                    {diagramElProps.nodeDatum.attributes?.newLine}
                </text>
            )}
            {diagramElProps.nodeDatum.attributes?.level && (
                <text fill="black" x="50" fontSize={18} dy={diagramElProps.nodeDatum.attributes?.newLine?"50":"40"} strokeWidth="1">
                    level: {diagramElProps.nodeDatum.attributes?.level}
                </text>
            )}
        </g>
    );
}

const _colorPicker =(level:string | number | boolean)=>{
    switch (level) {
        case "1":
            return "rgba(141,158,145,0.8)"; //"#8D9E91";
        case "2":
            return "rgba(0, 140, 212, 0.7)"; //"rgba(139,122,140,0.3)"; //"#8B7A8C";
        case "3":
            return "rgba(179,130,181,0.8)"; //"#B382B5";
    }
}

const getImagePlanOperator = (planOperator:string | number | boolean | undefined) => {

    let image;
    switch (planOperator){
        case 'PROJECT':
            image=projection;
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
        case 'UNNEST':
            image=unnest;
            break;
        case 'JOIN':
            image=equiJoin;
            break;
        default:
            return;
    }
    return <img src={image} alt={'planOperatorSymbol'} width={30} height={30}/>
}

export default ShreddedPlan;