import React from "react";
import Popover from '@material-ui/core/Popover';
import Tree from 'react-d3-tree';
import {createStyles, makeStyles} from "@material-ui/core/styles";

interface _PopoverTreeDiagramProps{
    isOpen : boolean;
}

const orgChart = {
    name: 'Sample,mutations',
    attributes: {
        Level: '1',
    },
    children: [
        {
            name: 'mutId, candidates, sID, sample',
            attributes: {
                Level: '2',
            },
            children: [
                {
                    name: 'gene,score,sID,sample,mutId',
                    attributes: {
                        Level: '2',
                    },
                    children: [
                        {
                            name: 'impact*(cnum+0.01)*sift*polysID,sample,mutId,gene',
                            attributes: {
                                Level: '3',
                            },
                            children:[
                                {
                                    name:'sample.gene',
                                    attributes: {
                                        Level: '3',
                                    },
                                    children:[
                                        {
                                            name:'candidates',
                                            attributes: {
                                                Level: '3',
                                            },
                                            children:[
                                                {
                                                    name:'sample',
                                                    attributes: {
                                                        Level: '2',
                                                    },
                                                    children:[
                                                        {name: 'Occurrences',
                                                            attributes: {
                                                                Level: '1',
                                                            }},
                                                        {name:'Samples',
                                                            attributes: {
                                                                Level: '1',
                                                            }}
                                                    ]
                                                }
                                            ]
                                        },
                                        {
                                            name:'CopyNumber',
                                            attributes: {
                                                Level: '3',
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


const PopoverTreeDiagram = (props:_PopoverTreeDiagramProps) => {

    return (
        <Popover
            PaperProps={{
                style: { width: '500px',height:'1000px' }
            }}
            anchorReference="anchorPosition"
            anchorPosition={{ top: 200, left: 400 }}
            anchorOrigin={{
                vertical: 'center',
                horizontal: 'left',
            }}
            transformOrigin={{
                vertical: 'center',
                horizontal: 'left',
            }}
            open={props.isOpen}
        >
            <Tree
                data={orgChart} orientation={"vertical"}
                nodeSize={{x:200, y:120}}
                pathFunc={"straight"}
                zoom={0.7}
                enableLegacyTransitions
                translate={{x:200,y:20}}
                transitionDuration={1500}
            />
        </Popover>
    )
}

export default PopoverTreeDiagram;