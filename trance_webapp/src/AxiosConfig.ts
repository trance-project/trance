import axios from 'axios';

const instance = axios.create({
    baseURL: 'http://localhost:3000/',
    withCredentials: true,
    headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json;charset=UTF-8'
    }
});

export const trancePlayInstance = axios.create({
    baseURL: "http://localhost:9000/",
    withCredentials: true,
    headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json;charset=UTF-8',
    }
})

export  default instance;