const INITIAL_XML = '<xml xmlns="http://www.w3.org/1999/xhtml"></xml>';

const INITIAL_TOOLBOX_JSON = {
  kind: "categoryToolbox",
  contents: [
    {
      kind: "category",
      name: "Logic",
      colour: "#5C81A6",
      contents: [
        {
          kind: "block",
          type: "controls_if",
        },
        {
          kind: "block",
          type: "logic_compare",
        },
      ],
    },
    {
      kind: "category",
      name:"nrc.1.0",
      colour:210,
      contents: [
        {
          kind: "block",
          type:"forunion",
          // blockxml: '<block type="forunion"/>'
        },
        {
          kind: "block",
          type:"tuple",
          // blockxml: '<block type="tuple"/>'
        },
        {
          kind: "block",
          type:"customReactComponent",
          // blockxml: '<block type="customReactComponent"/>'
        },
        {
          kind: "block",
          type:"groupBy",
          // blockxml: '<block type="brackets"/>'
        },
        {
          kind: "block",
          type:"sumBy",
          // blockxml: '<block type="brackets"/>'
        },
        {
          kind: "block",
          type:"and",
          // blockxml: '<block type="and"/>'
        },
        {
          kind: "block",
          type:"or",
          // blockxml: '<block type="or"/>'
        },
        {
          kind: "block",
          type:"tuple_el",
          // blockxml: '<block type="tuple_el"/>'
        },
        {
          kind: "block",
          type:"tuple_el_iteration",
          // blockxml: '<block type="tuple_el_iteration"/>'
        },
        {
          kind: "block",
          type:"ifstmt_primitive",
          // blockxml: '<block type="ifstmt_primitive"/>'
        },
        {
          kind: "block",
          type:"ifstmt_bag",
          // blockxml: '<block type="ifstmt_bag"/>'
        },
        {
          kind: "block",
          type:"association_on",
          // blockxml: '<block type="association_on"/>'
        },
        {
          kind: "block",
          type:"association_on_assisted",
          // blockxml: '<block type="association_on_assisted"/>'
        },
        {
          kind: "block",
          type:"group_by",
          // blockxml: '<block type="group_by"/>'
        }
      ]
    },
    {
      kind: "category",
      name: "Custom",
      colour: "#5CA699",
      contents: [
        {
          kind: "block",
          type: "new_boundary_function",
        },
        {
          kind: "block",
          type: "return",
        },
      ],
    },
  ]
}

const INITIAL_TOOLBOX_XML = '<xml xmlns="http://www.w3.org/1999/xhtml" id="toolbox" style="display: none;">\n'
  // + '  <category name="Logic" colour="#5C81A6">\n'
  // + '    <block type="controls_if"></block>\n'
  // + '    <block type="logic_compare">\n'
  // + '      <field name="OP">EQ</field>\n'
  // + '    </block>\n'
  // + '    <block type="logic_operation">\n'
  // + '      <field name="OP">AND</field>\n'
  // + '    </block>\n'
  // + '    <block type="logic_negate"></block>\n'
  // + '    <block type="logic_boolean">\n'
  // + '      <field name="BOOL">TRUE</field>\n'
  // + '    </block>\n'
  // + '    <block type="logic_null"></block>\n'
  // + '    <block type="logic_ternary"></block>\n'
  // + '  </category>\n'
  // + '  <category name="Loops" colour="#5CA65C">\n'
  // + '    <block type="controls_repeat_ext">\n'
  // + '      <value name="TIMES">\n'
  // + '        <shadow type="math_number">\n'
  // + '          <field name="NUM">10</field>\n'
  // + '        </shadow>\n'
  // + '      </value>\n'
  // + '    </block>\n'
  // + '    <block type="controls_whileUntil">\n'
  // + '      <field name="MODE">WHILE</field>\n'
  // + '    </block>\n'
  // + '    <block type="controls_for">\n'
  // + '      <field name="VAR" id="C(8;cYCF}~vSgkxzJ+{O" variabletype="">i</field>\n'
  // + '      <value name="FROM">\n'
  // + '        <shadow type="math_number">\n'
  // + '          <field name="NUM">1</field>\n'
  // + '        </shadow>\n'
  // + '      </value>\n'
  // + '      <value name="TO">\n'
  // + '        <shadow type="math_number">\n'
  // + '          <field name="NUM">10</field>\n'
  // + '        </shadow>\n'
  // + '      </value>\n'
  // + '      <value name="BY">\n'
  // + '        <shadow type="math_number">\n'
  // + '          <field name="NUM">1</field>\n'
  // + '        </shadow>\n'
  // + '      </value>\n'
  // + '    </block>\n'
  // + '    <block type="controls_forEach">\n'
  // + '      <field name="VAR" id="Cg!CSk/ZJo2XQN3=VVrz" variabletype="">j</field>\n'
  // + '    </block>\n'
  // + '    <block type="controls_flow_statements">\n'
  // + '      <field name="FLOW">BREAK</field>\n'
  // + '    </block>\n'
  // + '  </category>\n'
  // + '  <category name="Nrc" colour="#5C68A6">\n'
    + '  <block type="forunion"/>\n'
  + '    <block type="forUnionNested"/>\n'
  + '    <block type="tuple"/>\n'
  + '    <block type="customReactComponent"/>\n'
  // + '    <block type="brackets"/>\n'
  + '    <block type="groupBy"/>\n'
  + '    <block type="sumBy"/>\n'
  + '    <block type="and"/>\n'
  + '    <block type="or"/>\n'
  + '    <block type="tuple_el"/>\n'
  + '    <block type="tuple_el_iteration"/>\n'
  + '    <block type="controls_if"></block>\n'
  // + '    <block type="ifstmt_primitive"/>\n'
  // + '    <block type="ifstmt_bag"/>\n'
  // + '    <block type="association_on"/>\n'
  // + '    <block type="association_on_assisted"/>\n'
  + '    <block type="association_on_assisted_new"/>\n'
  + '    <block type="association_on_assisted_new_new"/>\n'
  + '    <block type="text_input"/>\n'
  + '    <block type="text_statement"/>\n'
  + '    <block type="nrc_filters"/>\n'
  // + '  </category>\n'
  + '</xml>';

const INITIAL_TOOLBOX = [
  {type: "forunion"},
  {type: "forUnionNested"},
  {type: "tuple"},
  {type: "customReactComponent"},
  {type: "groupBy"},
  {type: "sumBy"},
  {type: "and"},
  {type: "or"},
  {type: "tuple_el"},
  {type: "tuple_el_iteration"},
  {type: "controls_if"},
  {type: "association_on_assisted_new"},
  {type: "association_on_assisted_new_new"},
  {type: "text_input"},
  {type: "text_statement"},
  {type: "nrc_filters"}
];

// const INITIAL_XML =  '<xml xmlns="https://developers.google.com/blockly/xml"><block type="forunion" id="^cl43)XAq+ypXo~6N@n0" x="90" y="30"><field name="OBJECT_KEY">s</field><field name="ATTRIBUTE_VALUE">Sample</field><next><block type="tuple" id="=[.(!edr!_W4{^%$V{TU"><statement name="ATTRIBUTES"><block type="tuple_el" id="{t[{?T.WB8E/H=7(XVHa"><field name="ATTRIBUTE_NAME">sample</field><value name="ATTRIBUTE_VALUE"><block type="text" id="ZFxtKAaTR;w_(!b5koSo"><field name="TEXT">s.Sample,</field></block></value><next><block type="tuple_el_iteration" id="2P1o%kVxGNiq4jA7H/CY"><field name="ATTRIBUTE_NAME">mutation</field><statement name="ATTRIBUTE_VALUE"><block type="forunion" id="0?}7EM=Bq-Zzip4[EW_D"><field name="OBJECT_KEY">o</field><field name="ATTRIBUTE_VALUE">Occurrences</field><next><block type="object_association" id="Q8xsl+:5AJ/`{ZZ(EYX8"><statement name="OBJECT_ASSOCIATION"><block type="association_on" id="WbprI4.AqEQ~mZ;=J)GU"><field name="ATTRIBUTE_A">s.sample</field><field name="ATTRIBUTE_B">o.sample</field></block></statement><next><block type="tuple" id="4:=?*w=j]T)wC$S`~gAC"><statement name="ATTRIBUTES"><block type="tuple_el" id="oSz=H8?XvV^U/iz)yEK4"><field name="ATTRIBUTE_NAME">mutId</field><value name="ATTRIBUTE_VALUE"><block type="text" id="kI[QTAX-AItta-5iDNU6"><field name="TEXT">o.mutId</field></block></value><next><block type="tuple_el_iteration" id="y[d/`mO+I!*$[9ScutDR"><field name="ATTRIBUTE_NAME">scores</field><statement name="ATTRIBUTE_VALUE"><block type="group_by" id="oO_;oyV]TrEmKev*ul6f"><field name="ATTRIBUTE_KEY">score</field><field name="ATTRIBUTE_VALUE">gene</field><next><block type="brackets" id="KTjd4~AnaJDH6J4V6,ky"><statement name="GROUP_BY"><block type="forunion" id=".}p1%Q.bsbvM8c~O1FsA"><field name="OBJECT_KEY">t</field><field name="ATTRIBUTE_VALUE">o.candidates</field><next><block type="forunion" id="*g6J.^Yoi8Y/,/+v[Vb8"><field name="OBJECT_KEY">c</field><field name="ATTRIBUTE_VALUE">CopyNumber</field><next><block type="object_association" id="/3]}xJK3G_MJt^Oz;jIw"><statement name="OBJECT_ASSOCIATION"><block type="association_on" id="N}t){bqEsQp4shwyJ4ZH"><field name="ATTRIBUTE_A">t.gene</field><field name="ATTRIBUTE_B">c.gene</field><next><block type="and" id="QC6}bS?IpEKiz}YCqEF]"><next><block type="association_on" id="FC?/%FP,5v.fYmzjv;Vh"><field name="ATTRIBUTE_A">s.sample</field><field name="ATTRIBUTE_B">o.sample</field></block></next></block></next></block></statement><next><block type="tuple" id="Xf+en{Cnt0X%O;Wc@oex"><statement name="ATTRIBUTES"><block type="tuple_el" id="!};mhVuP3UX:/w87YH0_"><field name="ATTRIBUTE_NAME">gene</field><value name="ATTRIBUTE_VALUE"><block type="text" id="}6.+#hWZVK`?L^r#Y+jX"><field name="TEXT">t.gene,</field></block></value><next><block type="tuple_el" id="Wd8T%w@+F,f`1i6udA,R"><field name="ATTRIBUTE_NAME">score</field><value name="ATTRIBUTE_VALUE"><block type="text" id="|PgH_`x^XkzKE[`#n]o+"><field name="TEXT">t.impact * (c.cnum + 0.01) * t.sift * t.poly</field></block></value></block></next></block></statement></block></next></block></next></block></next></block></statement></block></next></block></statement></block></next></block></statement></block></next></block></next></block></statement></block></next></block></statement></block></next></block></xml>';

const ConfigFiles = {
  INITIAL_XML,
  INITIAL_TOOLBOX_XML,
  INITIAL_TOOLBOX,
  INITIAL_TOOLBOX_JSON
};

export default ConfigFiles;



