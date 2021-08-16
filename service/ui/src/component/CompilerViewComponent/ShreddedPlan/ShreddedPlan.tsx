import React from "react";
import {Accordion, AccordionDetails, Grid} from "@material-ui/core";
import Tree from "react-d3-tree";
import {CustomNodeElementProps, RawNodeDatum} from "react-d3-tree/lib/types/common";

import projection from "../../../static/images/planOperator/Projection.png";
import nest from "../../../static/images/planOperator/Nest.png";
import sumAggregate from "../../../static/images/planOperator/Sum_aggregate.png";
import leftOuterJoin from "../../../static/images/planOperator/LeftOuterJoin.png";
import unnest from "../../../static/images/planOperator/Unnest.png";
import equiJoin from "../../../static/images/planOperator/Equijoin.png";
import {useAppSelector,useAppDispatch} from '../../../redux/Hooks/hooks';
import {runShredPlan} from '../../../redux/QuerySlice/thunkQueryApiCalls';
import Button from "@material-ui/core/Button";
import ForwardIcon from "@material-ui/icons/Forward";
import {NewQuery,LabelType} from "../../../utils/Public_Interfaces";
import NewLabelView from "../../Query/QueryBuilderComponents/StandardCompilationBuilder/LabelView/NewLabelView";

interface _ShreddedPlanProps{
    translate: {x:number, y:number};
    zoom:number;
}
const ShreddedPlan = (props:_ShreddedPlanProps) => {

    const dispatch = useAppDispatch();

    const shreddedResponse = useAppSelector(state => state.query.shreddedResponse);
    const selectedQuery = useAppSelector(state => state.query.selectedQuery);
    const nrcCode = useAppSelector(state => state.query.nrcQuery);

    let planElement: JSX.Element[] = [];

    const handleButtonClick = () => {
        dispatch(runShredPlan({
            _id: selectedQuery!._id,
                body: nrcCode,
                title: selectedQuery!.name
        }))
    }
    if(shreddedResponse){
      planElement = shreddedResponse.shred_plan.map((el,index) => {
          const depth = getNestedDepth(el);
          return (
              <Grid item key={index} xs={12} style={{height: `${(depth * 100)}px`}}>
                  <Tree
                      data={el}
                      orientation={"vertical"}
                      pathFunc={"straight"}
                      zoom={props.zoom}
                      enableLegacyTransitions
                      translate={props.translate}
                      transitionDuration={1500}
                      renderCustomNodeElement={diagramProps => renderNodeWithCustomEvents(diagramProps, index)}
                      separation={{siblings: 5.25}}
                      zoomable={false}
                  />
              </Grid>
          )
      })
    }
    return (
        <React.Fragment>
            <Grid container direction={"row"} style={{height: "670px"}}>
                {planElement}
                <Button variant={"contained"} color={"primary"} onClick={handleButtonClick} endIcon={<ForwardIcon/>}>Run</Button>
            </Grid>
        </React.Fragment>
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
const renderNodeWithCustomEvents = (diagramElProps: CustomNodeElementProps, index:number) => {
    // @ts-ignore
    const level = parseInt(diagramElProps.nodeDatum.attributes?.level!);
    const levelColor = _colorPicker(index);
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
            {level >= 0 && (
                <text fill="black" x="50" fontSize={18} dy={diagramElProps.nodeDatum.attributes?.newLine?"50":"40"} strokeWidth="1">
                    level: {diagramElProps.nodeDatum.attributes?.level}
                </text>
            )}
        </g>
    );
}

//todo add to global state
const _colorPicker =(level:number )=>{
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
        case 5:
            return "rgba(40,165,5,0.8)";
        case 6:
            return "rgba(141,5,165,0.8)";
        case 7:
            return "rgba(189,148,9,0.8)";
        case 8:
            return "rgba(188,212,24,0.8)";
        case 9:
            return "rgba(5,165,120,0.8)";
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


const getNestedDepth = (jsonObject: RawNodeDatum, depth = 1,index =0) => {
    let results = 0;
    if(jsonObject.children?.length == 0){
        return depth
    }else{
        results = getNestedDepth(jsonObject.children![index], depth +1, 0)
    };
    return results;

}

export default ShreddedPlan;