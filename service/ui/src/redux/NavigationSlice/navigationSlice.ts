import {createSlice, PayloadAction} from '@reduxjs/toolkit';

import {pageRoutes} from '../../utils/Public_enums';


/**
 * Defined a type for the slice type
 */
interface NavigationState {
    activePage: pageRoutes;
}

/**
 * Defined the initial state using that type
 */
const initialState: NavigationState = {
    activePage: pageRoutes.DASHBOARD
}


/**
 * Reducer slice for managing the navigation styling on layout high order component
 */
export const navigationSlice = createSlice({
    name: 'selectedPage',
    initialState,
    reducers: {
        // Use the PayloadAction type to declare the contents of `action.payload`
        goToRoute: (state, action: PayloadAction<pageRoutes>) => {
            state.activePage = action.payload
        }
    }
});


/**
 * Action Creators are generated for each reducer function
 */
export const {goToRoute} = navigationSlice.actions;

export default navigationSlice.reducer;