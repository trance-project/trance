import React from "react";
import Tree from "react-d3-tree";
import Dialog from '@material-ui/core/Dialog';
import {CustomNodeElementProps, RawNodeDatum} from "react-d3-tree/lib/types/common";


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
            <text fill="black" strokeWidth="0.5" x="20" onClick={diagramElProps.toggleNode}>
                {diagramElProps.nodeDatum.name}
            </text>
            {diagramElProps.nodeDatum.attributes?.newLine && (
                <text fill="black" x="20" dy="20" strokeWidth="0.5">
                    {diagramElProps.nodeDatum.attributes?.newLine}
                </text>
            )}
            {diagramElProps.nodeDatum.attributes?.level && (
                <text fill="black" x="20" dy={diagramElProps.nodeDatum.attributes?.newLine?"40":"20"} strokeWidth="1">
                    level: {diagramElProps.nodeDatum.attributes?.level}
                </text>
            )}
        </g>
    );
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

interface _TreeDiagramProps{
    open: boolean;
    close:() => void;
    treeData: RawNodeDatum;
}

const TreeDiagram = (props:_TreeDiagramProps) => (
    <Dialog open={props.open}
            PaperProps={{
                style: { width: '500px',height:'1000px' }
            }}
            onClose={props.close}
    >
        <Tree
            data={props.treeData}
            orientation={"vertical"}
            pathFunc={"straight"}
            zoom={0.65}
            enableLegacyTransitions
            translate={{x:200, y:20}}
            transitionDuration={1500}
            renderCustomNodeElement={diagramProps => renderNodeWithCustomEvents(diagramProps)}
            separation={{siblings:1.7}}
        />
    </Dialog>
);

export default TreeDiagram;