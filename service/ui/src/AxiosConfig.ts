import axios from 'axios';
import {config} from './Constants'

const instance = axios.create({
    baseURL: 'http://localhost:3000/',
    withCredentials: true,
    headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json;charset=UTF-8'
    }
});

export const trancePlayInstance = axios.create({
    baseURL: config.url,
    // baseURL: "http://pdq-webapp.cs.ox.ac.uk:3000",
    withCredentials: true,
    headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json;charset=UTF-8',
    }
})

export  default instance;