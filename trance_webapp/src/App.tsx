import React from 'react';

import './App.css';
import {tranceTheme} from "./hoc/TranceTheme/TranceTheme";
import {ThemeProvider} from "@material-ui/core/styles";
import TranceRouter from "./TranceRouter";


function App() {

  return (
      <ThemeProvider theme={tranceTheme}>
        <TranceRouter/>
      </ThemeProvider>
  );
}

export default App;
