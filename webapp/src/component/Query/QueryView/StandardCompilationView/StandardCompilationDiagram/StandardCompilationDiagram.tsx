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
            <text fill="black" strokeWidth="0.5" x="50" y={"20"} fontSize={27} onClick={diagramElProps.toggleNode}>
                {diagramElProps.nodeDatum.name}
            </text>
            {diagramElProps.nodeDatum.attributes?.newLine && (
                <text fill="black" x="50" dy="40" fontSize={25} strokeWidth="0.5">
                    {diagramElProps.nodeDatum.attributes?.newLine}
                </text>
            )}
            {diagramElProps.nodeDatum.attributes?.level && (
                <text fill="black" x="50" fontSize={18} dy={diagramElProps.nodeDatum.attributes?.newLine?"60":"40"} strokeWidth="1">
                    level: {diagramElProps.nodeDatum.attributes?.level}
                </text>
            )}
        </g>
    );
}

const getImagePlanOperator = (planOperator:String | undefined) => {

    let image;
    switch (planOperator){
        case 'projection':
            image=projection;
            break;
        case 'nest':
            image=nest;
            break;
        case 'sum-aggregate':
            image=sumAggregate;
            break;
        case 'left-outer-join':
            image=leftOuterJoin;
            break;
         case 'outer-unnest':
            image=outerUnnest;
            break;
        case 'unnest':
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
            return "#8D9E91";
        case "2":
            return "#8B7A8C";
        case "3":
            return "#B382B5";
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
        separation={{siblings:1.7}}
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
        planOperator: 'projection'
    },
    children: [
        {
            name: '',
            attributes: {
                newLine: 'mutId, candidates, sID, sample',
                level: '2',
                planOperator:'nest'
            },
            children: [
                {
                    name: '',
                    attributes: {
                        newLine: 'gene, score, sID, sample, mutId',
                        level: '2',
                        planOperator:'nest'
                    },
                    children: [
                        {
                            name: 'impact*(cnum+0.01)*sift*poly',
                            attributes: {
                                newLine:'sample, gene, label, mutId, sID',
                                level: '3',
                                planOperator:'sum-aggregate'
                            },
                            children:[
                                {
                                    name:'',
                                    attributes: {
                                        newLine: 'sample, gene',
                                        level: '3',
                                        planOperator:'left-outer-join'
                                    },
                                    children:[
                                        {
                                            name:'',
                                            attributes: {
                                                newLine: 'candidates',
                                                level: '3',
                                                planOperator:'outer-unnest'
                                            },
                                            children:[
                                                {
                                                    name:'',
                                                    attributes: {
                                                        newLine: 'sample',
                                                        level: '2',
                                                        planOperator:'left-outer-join'
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

