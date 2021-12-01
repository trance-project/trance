const prod = {
    url: "http://pdq-webapp.cs.ox.ac.uk:3000",
    zepplineURL: "http://pdq-webapp.cs.ox.ac.uk:8085",
    apacheSpark: "http://pdq-webapp.cs.ox.ac.uk:18080/"
}

const dev = {
    url: "http://localhost:3000/",
    zepplineURL:"http://localhost:8085/",
    apacheSpark: "http://localhost:18080/"
}

export const config = process.env.NODE_ENV === `development` ? dev : prod;