/* eslint-disable import/no-extraneous-dependencies */
import '../../../../../../App.css';
import React from 'react';
import ReactBlockly from 'react-blockly';
import Blockly from 'blockly';
import BlocklyJS from 'blockly/javascript';
import Grid from "@material-ui/core/Grid";

import ConfigFiles from './initContent/content';
import parseWorkspaceXml from './BlocklyHelper';
import './customBlocks/custom_Blocks_old';
import './generator';
import SaveButton from "./BlocklyActionButtons/SaveButton";
import SendButton from "./BlocklyActionButtons/SendButton";
import {connect} from 'react-redux';
import {addToSelectedObjects, modifySelectedObjectKeyValue} from '../../../../../../redux/TranceObjectSlice/tranceObjectSlice'
import {updateBlocklyQuery, setNrcCode} from '../../../../../../redux/QuerySlice/querySlice';
import NrcCodeView from "./nrcCodeView/NrcCodeView";



class TestEditor extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            toolboxCategories: parseWorkspaceXml(ConfigFiles.INITIAL_TOOLBOX_XML),
            blocklyXml: "",
            nrcCode: ""
        };
    }

    shouldComponentUpdate =(nextProps, nextState) => {
        return (this.props.query.selectedQuery != nextProps.query.selectedQuery)
    }

    componentDidMount = () => {

    }





    workspaceDidChange = (workspace) => {
        const firstFunction = (event) => {
            if(event.type === Blockly.Events.CHANGE){
                // get block info from blockID
                const block = workspace.getBlockById(event.blockId);
                switch (block.type){
                    case "forunion" : {
                        switch (event.name){
                            case "ATTRIBUTE_VALUE" :{
                                if(event.oldValue !== event.newValue){
                                    this.addTranceObjectByName(event.newValue)
                                }
                                break;
                            }
                            case "OBJECT_KEY" : {
                                console.log('[block]', block.getFieldValue( 'ATTRIBUTE_VALUE'));
                                const block_attribute_value = block.getFieldValue( 'ATTRIBUTE_VALUE');
                                if(block_attribute_value !== "null"){
                                    this.modifyTranceObjectByName(block_attribute_value, event.newValue)
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
        this.props.updateBlocklyQuery(newXml);

        const code = BlocklyJS.workspaceToCode(workspace);
        console.log("[code]" , code)
        this.setState({
            nrcCode:code,
            blocklyXml: newXml
        })
        this.props.setNrcCode(code);
        console.log(this.state.nrcCode)
        console.log(this.state.blocklyXml)
    }

    addTranceObjectByName = (objectName) => {
        const object = this.props.tranceObject.objects.find(o => objectName===o.name)
        const selectedObjects = this.props.tranceObject.selectedObjects;
        let foundObject;
        if(selectedObjects && object){
            foundObject =  selectedObjects.find(o => o._id === object._id)
            if(!foundObject){
                this.props.addToSelectedObjects(object)
            }
        }
    }

    modifyTranceObjectByName = (objectName, object_key) => {
        const object = this.props.tranceObject.selectedObjects.find(o => objectName===o.name)
        if(object && object.abr !== object_key){
            const newObject = JSON.parse(JSON.stringify(object));
            newObject.abr = object_key;
            if(object.abr !== newObject.abr){
                console.log("[object.abr]", object.abr)
                console.log("[newObject.abr]",newObject.abr)
                this.props.modifySelectedObjectKeyValue(newObject);
            }

        }
    }

    render = () => {
        console.log("[state for nrc code gen]", this.state.nrcCode)
        console.log("[props for init XMl]", this.props.query.selectedQuery)
        console.log('[props]',this.props)
        const selectedQueryBlock = this.props.query.selectedQuery;
        const blockly = selectedQueryBlock?
            (
                <>
                <Grid container spacing={2}>
                    <Grid item md={9} xs={12}>
                        <ReactBlockly
                            key={selectedQueryBlock._id}
                            toolboxCategories={this.state.toolboxCategories}
                            workspaceConfiguration={{
                                grid: {
                                    spacing: 20,
                                    length: 10,
                                    colour: '#ccc',
                                    snap: true,
                                },
                            }}
                            initialXml={selectedQueryBlock.xmlDocument}
                            wrapperDivClassName="fill-height"
                            workspaceDidChange={this.workspaceDidChange}

                        />
                        <SaveButton />
                    </Grid>
                    <Grid item md={3} xs={12}>
                        <NrcCodeView id={selectedQueryBlock._id} nrcCode={this.state.nrcCode}/>
                        <SendButton />
                    </Grid>
                </Grid>
                </>
            ) :<h1>Select or create a new query</h1>;
        return blockly;
    }

}

const mapStateToProps = ({tranceObject: tranceObject, query:query}) => ({
    tranceObject,
    query
})

const mapDispatchToProps = {
    addToSelectedObjects,
    modifySelectedObjectKeyValue,
    updateBlocklyQuery,
    setNrcCode
}



export default connect(mapStateToProps, mapDispatchToProps)(TestEditor);




