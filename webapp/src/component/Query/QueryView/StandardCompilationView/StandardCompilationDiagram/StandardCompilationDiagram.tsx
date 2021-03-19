import React from "react";
import Tree from "react-d3-tree";
import {CustomNodeElementProps, RawNodeDatum} from "react-d3-tree/lib/types/common";
import {Grid} from "@material-ui/core";

import projection from '../../../../../static/images/planOperator/Projection.png';
import nest from '../../../../../static/images/planOperator/Nest.png';
import sumAggregate from '../../../../../static/images/planOperator/Sum_aggregate.png';
import leftOuterJoin from '../../../../../static/images/planOperator/LeftOuterJoin.png';
import outerUnnest from '../../../../../static/images/planOperator/outer-unest.png';
import unnest from "../../../../../static/images/planOperator/Unnest.png";


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

const getImagePlanOperator = (planOperator:String | undefined) => {

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

const _colorPicker =(level:String)=>{
    switch (level) {
        case "1":
            return "#8D9E91"; //"rgba(141,158,145,0.3)";
        case "2":
            return "rgba(0, 140, 212, 0.7)"; //"#8B7A8C"; //"rgba(139,122,140,0.3)"; 
        case "3":
            return "#B382B5"; //"rgba(179,130,181,0.3)"; 
    }
}

const StandardCompilationDiagram = () => (
    <Grid item xs={12} direction={"row"} style={{height:700}}>
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
        zoomable={false}
    />
    </Grid>
)

export default StandardCompilationDiagram;

const treeDiagramData:RawNodeDatum ={
    name: '',
    attributes: {
        newLine: 'sample, mutations',
        level: '1',
        planOperator: 'PROJECT'
    },
    children: [
        {
            name: '',
            attributes: {
                newLine: 'mutId, candidates, sID, sample',
                level: '2',
                planOperator:'NEST'
            },
            children: [
                {
                    name: '',
                    attributes: {
                        newLine: 'gene, score, sID, sample, mutId',
                        level: '2',
                        planOperator:'NEST'
                    },
                    children: [
                        {
                            name: '', //impact*(cnum+0.01)*sift*poly',
                            attributes: {
                                newLine:'sample, gene, label, mutId, sID',
                                level: '3',
                                planOperator:'SUM'
                            },
                            children:[
                                {
                                    name:'',
                                    attributes: {
                                        newLine: 'sample, gene',
                                        level: '3',
                                        planOperator:'OUTERJOIN'
                                    },
                                    children:[
                                        {
                                            name:'',
                                            attributes: {
                                                newLine: 'candidates',
                                                level: '3',
                                                planOperator:'OUTERUNNEST'
                                            },
                                            children:[
                                                {
                                                    name:'',
                                                    attributes: {
                                                        newLine: 'sample',
                                                        level: '2',
                                                        planOperator:'OUTERJOIN'
                                                    },
                                                    children:[
                                                        {name: 'Occurrences',
                                                            attributes: {
                                                                level: '1',
                                                            }},
                                                        {name:'Samples',
                                                            attributes: {
                                                                level: '1',
                                                            }}
                                                    ]
                                                }
                                            ]
                                        },
                                        {
                                            name:'CopyNumber',
                                            attributes: {
                                                level: '3',
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                    ],
                },
            ],
        },
    ],
};

