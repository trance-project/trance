/**
 * React Blockly v7 (unstable can't generate custom blocks yet)
 * @URL https://github.com/nbudin/react-blockly
 * @version react-blockly v7.0.0-alpha.1
 */

import React, {useState, useCallback, useEffect} from "react";
import Blockly from "blockly";

import "./customBlocks/custom_Blocks_old";
import "./generator";
import ConfigFiles from './initContent/content';
import {BlocklyWorkspace} from "react-blockly";
import BlocklyJS from "blockly/javascript";


const BlocklyComponent = () => {
    const [toolboxConfiguration, setToolboxConfiguration] = useState(ConfigFiles.INITIAL_TOOLBOX_JSON);
    const [blocklyXml, setBlocklyXml] = useState("");
    const [NRCCode, setNRCCode] = useState("");

    /**
     * function gets called when every time a new change happens on the blockly grid
     * @type {(function(*=): void)|*}
     */
    const onWorkspaceChange = useCallback((workspace) => {
        const firstFunction = (event) => {
            if(event.type === Blockly.Events.CHANGE){
                // get block info from blockID
                const block = workspace.getBlockById(event.blockId);
                switch (block.type){
                    case "forunion" : {
                        switch (event.name){
                            case "ATTRIBUTE_VALUE" :{
                                if(event.oldValue !== event.newValue){
                                    addTranceObjectByName(event.newValue)
                                }
                                break;
                            }
                            case "OBJECT_KEY" : {
                                console.log('[block]', block.getFieldValue( 'ATTRIBUTE_VALUE'));
                                const block_attribute_value = block.getFieldValue( 'ATTRIBUTE_VALUE');
                                if(block_attribute_value !== "null"){
                                    modifyTranceObjectByName(block_attribute_value, event.newValue)
                                }
                                break;
                            }
                        }
                        break;
                    }
                }

            }
        }
        workspace.addChangeListener(firstFunction);
        console.log("[workspaceDidChange]", workspace);
        const newXml = Blockly.Xml.domToText(Blockly.Xml.workspaceToDom(workspace));
        // this.props.updateBlocklyQuery(newXml);

        const code = BlocklyJS.workspaceToCode(workspace);
        console.log("[code]" , code)

        setBlocklyXml(newXml);
        setNRCCode(code)
        console.log("[NRC Code]",NRCCode)
        console.log("[Blockly Xml]",blocklyXml)
    }, []);

    /**
     * event trigger on every XMl change in blockly Grid
     * @type {(function(*): void)|*}
     */
    // const onXmlChange = useCallback((newXml) => {
    //     document.getElementById("generated-xml").innerText = newXml;
    // }, []);

    const addTranceObjectByName = (objectName) => {
        // const object = this.props.tranceObject.objects.find(o => objectName===o.name)
        // const selectedObjects = this.props.tranceObject.selectedObjects;
        // let foundObject;
        // if(selectedObjects && object){
        //     foundObject =  selectedObjects.find(o => o._id === object._id)
        //     if(!foundObject){
        //         this.props.addToSelectedObjects(object)
        //     }
        // }
    }

    const modifyTranceObjectByName = (objectName, object_key) => {
        // const object = this.props.tranceObject.selectedObjects.find(o => objectName===o.name)
        // if(object && object.abr !== object_key){
        //     const newObject = JSON.parse(JSON.stringify(object));
        //     newObject.abr = object_key;
        //     if(object.abr !== newObject.abr){
        //         console.log("[object.abr]", object.abr)
        //         console.log("[newObject.abr]",newObject.abr)
        //         this.props.modifySelectedObjectKeyValue(newObject);
        //     }
        //
        // }
    }
    return (
        <BlocklyWorkspace
            toolboxConfiguration={toolboxConfiguration}
            workspaceConfiguration={{
                grid: {
                    spacing: 20,
                    length: 3,
                    colour: "#ccc",
                    snap: true,
                },
            }}
            initialXml={ConfigFiles.INITIAL_XML}
            className="fill-height"
            onWorkspaceChange={onWorkspaceChange}
            // onXmlChange={onXmlChange}
        />
    );
}

export default BlocklyComponent