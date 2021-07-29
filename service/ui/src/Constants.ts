const prod = {
    url: "http://pdq-webapp.cs.ox.ac.uk:3000"
}

const dev = {
    url: "http://localhost:3000/"
}

export const config = process.env.NODE_ENV === `development` ? dev : prod;