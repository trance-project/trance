import React, {useState,useEffect, useRef}  from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Brush,
  ResponsiveContainer,
} from 'recharts';
import Typography from "@material-ui/core/Typography";
import * as d3 from 'd3';

import {RunTimeMetrics} from '../../../../utils/Public_Interfaces';



interface _SimpleAreaGraphVShreddedProps{
    data: RunTimeMetrics[];
}

const SimpleAreaGraphVShredded = (props:_SimpleAreaGraphVShreddedProps) => {

    const [lineChartPath, setLineChartPath] = useState<{path: any, fill:string}[]>()
    const [xScale, setXScale] = useState<number[] & d3.ScaleLinear<number, number>>()
    const [yScale, setYScale] = useState<number[] & d3.ScaleLinear<number, number>>()

    // @ts-ignore
    const xAxis = d3.axisBottom();
    const xAxisRef = useRef(null);
    // @ts-ignore
    const yAxis = useRef(d3.axisLeft());

    useEffect(()=>{
        const extentX = d3.extent(props.data, d=>d.pid);

        const xScale = d3.scaleTime()
            // @ts-ignore
            .domain(extentX).range([0,1050])

        const highMax = d3.max(props.data, d => d.shred_write_size + 500)
        const lowMin = d3.min(props.data, d => d.stand_write_size)


        const yScale = d3.scaleLinear()
            // @ts-ignore
            .domain([lowMin, highMax])
            .range([200, 0])

        // @ts-ignore
        const line = d3.line().x(d => xScale(d.pid))


        const linePath = [
            // @ts-ignore
            {path: line.y(d=>yScale(d.shred_write_size))(props.data), fill:"red"},
            // @ts-ignore
            {path: line.y(d=>yScale(d.stand_write_size))(props.data), fill:"blue"},
        ]

        // @ts-ignore
        setLineChartPath(linePath);

    }, []);

    return (
      <div style={{ width: '100%' }}>
        <Typography variant={'h6'}>Ordered by size</Typography>

        {/*<Typography>Standard</Typography>*/}
        {/*<ResponsiveContainer width="100%" height={200}>*/}
        {/*  <LineChart*/}
        {/*    width={500}*/}
        {/*    height={200}*/}
        {/*    data={props.data}*/}
        {/*    syncId="pid"*/}
        {/*    margin={{*/}
        {/*      top: 10,*/}
        {/*      right: 30,*/}
        {/*      left: 0,*/}
        {/*      bottom: 0,*/}
        {/*    }}*/}
        {/*  >*/}
        {/*    <CartesianGrid strokeDasharray="3 3" />*/}
        {/*    <XAxis dataKey="pid" domain={[0, 220]}/>*/}
        {/*    <YAxis />*/}
        {/*    <Tooltip />*/}
        {/*    <Line type="monotone" dataKey="stand_write_size" name="Size (KB)" stroke="#8884d8" fill="#8884d8" />*/}
        {/*  </LineChart>*/}
        {/*</ResponsiveContainer>*/}
        {/*<Typography>Shredded</Typography>*/}

        {/*<ResponsiveContainer width="100%" height={200}>*/}
        {/*  <LineChart*/}
        {/*    width={500}*/}
        {/*    height={200}*/}
        {/*    data={props.data}*/}
        {/*    syncId="pid"*/}
        {/*    margin={{*/}
        {/*      top: 10,*/}
        {/*      right: 30,*/}
        {/*      left: 0,*/}
        {/*      bottom: 0,*/}
        {/*    }}*/}
        {/*  >*/}
        {/*    <CartesianGrid strokeDasharray="3 3" />*/}
        {/*    <XAxis dataKey="pid" />*/}
        {/*    <YAxis name="Size (KB)" />*/}
        {/*    <Tooltip />*/}
        {/*    <Line type="monotone" dataKey="shred_write_size" name="Size (KB)" stroke="#82ca9d" fill="#82ca9d" />*/}
        {/*    <Brush />*/}
        {/*  </LineChart>*/}
        {/*</ResponsiveContainer>*/}
          <svg height={900} width={1500}>
              {lineChartPath?.map(d => (
                      <path d={d.path} fill={"none"} strokeWidth={2} stroke={d.fill}/>
                  )
              )}

          </svg>
      </div>
    );
}

export default SimpleAreaGraphVShredded