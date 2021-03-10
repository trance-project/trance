import React  from 'react';
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

interface _SimpleAreaGraphVShreddedProps{
    data: any[];
}

const SimpleAreaGraphVShredded = (props:_SimpleAreaGraphVShreddedProps) => {
    return (
      <div style={{ width: '100%' }}>
        <Typography variant={'h6'}>Ordered by size</Typography>

        <Typography>Standard</Typography>
        <ResponsiveContainer width="100%" height={200}>
          <LineChart
            width={500}
            height={200}
            data={props.data}
            syncId="pid"
            margin={{
              top: 10,
              right: 30,
              left: 0,
              bottom: 0,
            }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="pid" />
            <YAxis />
            <Tooltip />
            <Line type="monotone" dataKey="stand_write_size" name="Size (KB)" stroke="#8884d8" fill="#8884d8" />
          </LineChart>
        </ResponsiveContainer>
        <Typography>Shredded</Typography>

        <ResponsiveContainer width="100%" height={200}>
          <LineChart
            width={500}
            height={200}
            data={props.data}
            syncId="pid"
            margin={{
              top: 10,
              right: 30,
              left: 0,
              bottom: 0,
            }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="pid" />
            <YAxis name="Size (KB)"/>
            <Tooltip />
            <Line type="monotone" dataKey="shred_write_size" name="Size (KB)" stroke="#82ca9d" fill="#82ca9d" />
            <Brush />
          </LineChart>
        </ResponsiveContainer>

      </div>
    );
}

export default SimpleAreaGraphVShredded