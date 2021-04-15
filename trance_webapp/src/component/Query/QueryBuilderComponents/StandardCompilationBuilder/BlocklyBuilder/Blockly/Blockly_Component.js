/* eslint-disable import/no-extraneous-dependencies */
import '../../../../../../App.css';
import React from 'react';
import ReactBlockly from 'react-blockly'
import Blockly from 'blockly';
import BlocklyJS from 'blockly/javascript';
import ConfigFiles from './initContent/content';
import parseWorkspaceXml from './BlocklyHelper';
import './customBlocks/custom_Blocks';
import './generator';

class TestEditor extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            toolboxCategories: parseWorkspaceXml(ConfigFiles.INITIAL_TOOLBOX_XML),
        };
    }

    componentDidMount = () => {
        window.setTimeout(() => {
            this.setState({
                toolboxCategories: this.state.toolboxCategories.concat([
                    {
                        blocks: [
                            { type: 'text' },
                            {
                                type: 'text_print',
                                values: {
                                    TEXT: {
                                        type: 'text',
                                        shadow: true,
                                        fields: {
                                            TEXT: 'abc',
                                        },
                                    },
                                },
                            },
                        ],
                    },
                ]),
            });
        }, 2000);

        window.setTimeout(() => {
            this.setState({
                toolboxCategories: [
                    ...this.state.toolboxCategories.slice(0, this.state.toolboxCategories.length - 1),
                    {
                        ...this.state.toolboxCategories[this.state.toolboxCategories.length - 1],
                        blocks: [
                            { type: 'text' },
                        ],
                    },
                ],
            });
        }, 4000);

        window.setTimeout(() => {
            this.setState({
                toolboxCategories: this.state.toolboxCategories.slice(0, this.state.toolboxCategories.length - 1),
            });
        }, 10000);
    }

    workspaceDidChange = (workspace) => {
        workspace.registerButtonCallback('myFirstButtonPressed', () => {
            alert('button is pressed');
        });
        const newXml = Blockly.Xml.domToText(Blockly.Xml.workspaceToDom(workspace));
        document.getElementById('generated-xml').innerText = newXml;

        const code = BlocklyJS.workspaceToCode(workspace);
        document.getElementById('code').value = code;
    }

    render = () => (
        <React.Fragment>
            <ReactBlockly
            toolboxCategories={this.state.toolboxCategories}
            workspaceConfiguration={{
                grid: {
                    spacing: 20,
                    length: 3,
                    colour: '#ccc',
                    snap: true,
                },
            }}
            initialXml={ConfigFiles.INITIAL_XML}
            wrapperDivClassName="fill-height"
            workspaceDidChange={this.workspaceDidChange}
        />
            <pre id="generated-xml">
      </pre>
            <textarea id="code" style={{ height: "200px", width: "1300px" }} value=""></textarea>
        </React.Fragment>
    )
}

export default TestEditor;
