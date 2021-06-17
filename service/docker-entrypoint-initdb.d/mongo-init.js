
// Method used to hack the Mongo UUID object and only print out the UUID string
const generateUUID = () => {
    return UUID().toString().split('UUID(').join("").split(')').join("").split("\"").join("");
}
/*
    This is the init data when you would like to reset db state it should revert to this state
 */

/////////TRANCE_OBJECTS////////////////
const objects = [
    {
        _id: generateUUID(),
        name: "samples",
        abr:"s",
        columns:
            [
                {name: "bcr_patient_uuid", children:{}},
                {name: "turmorsite", children:{}},
                {name: "treatment_outcome", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "copyNumber",
        abr:"c",
        columns:
            [
                {name: "sample", children:{}},
                {name: "gene", children:{}} ,
                {name: "cnum", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "occurrences",
        abr:"o",
        columns:
            [
                {name: "sample", children:{}},
                {name: "contig", children:{}} ,
                {name: "end",  children:{}},
                {name: "reference", children:{}},
                {name: "alternate", children:{}},
                {name: "mutid", children:{}},
                {name: "candidates", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "candidates",
        abr:"cc",
        columns:
            [
                {name: "gene", children:{}},
                {name: "impact", children:{}} ,
                {name: "sift",  children:{}},
                {name: "poly", children:{}},
                {name: "consequences", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "consequences",
        abr:"cs",
        columns:
            [
                {name: "conseq", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "geneExpression",
        abr:"ge",
        columns:
            [
                {name: "id", children:{}},
                {name: "cname", children:{}},
                {name: "corders", children:{}},
                {name: "cdescription", children:{}},
                {name: "csequence", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "proteinInteractions",
        abr:"pi",
        columns:
            [
                {name: "id", children:{}},
                {name: "odate", children:{}},
                {name: "oparts", children:{}},
                {name: "odescription", children:{}},
                {name: "oclerk", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "drugResponse",
        abr:"dr",
        columns:
            [
                {name: "id", children:{}},
                {name: "nname", children:{}},
                {name: "ncusts", children:{}},
                {name: "ncountry", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "enhancers",
        abr:"e",
        columns:
            [
                {name: "id", children:{}},
                {name: "rname", children:{}},
                {name: "rnation", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "methylation",
        abr:"m",
        columns:
            [
                {name: "id", children:{}},
                {name: "qty", children:{}},
                {name: "orderid", children:{}},
                {name: "price", children:{}},
                {name: "deliveroption", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "genes",
        abr:"g",
        columns:
            [
                {name: "id", children:{}},
                {name: "pname", children:{}},
                {name: "pdescription", children:{}},
                {name: "pmanufacturer", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "pathways",
        abr:"p",
        columns:
            [
                {name: "id", children:{}},
                {name: "qty", children:{}},
                {name: "price", children:{}},
                {name: "description", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "variants",
        abr:"p",
        columns:
            [
                {name: "id", children:{}},
                {name: "sname", children:{}},
                {name: "sdescription", children:{}},
                {name: "sprice", children:{}},
            ]
    },

]
 const queryObject = [{
     _id: generateUUID(),
    title: "Test",
     body: "1234wjew"
 }]

///////////// END INIT DATA ///////////////////

print('Start #################################');

db = db.getSiblingDB('api_prod_db');
db.createUser(
    {
        user: 'trance_api_user',
        pwd: 'trance1234', // todo secure password
        roles: [{role: 'readWrite', db: 'api_prod_db'}]
    }
);
db.createCollection('users');

db = db.getSiblingDB('api_dev_db');
db.createUser(
    {
        user: 'trance_api_user',
        pwd: 'trance1234', // todo secure password
        roles: [{role: 'readWrite', db: 'api_dev_db'}]
    }
);
db.createCollection('trance_objects');
db.createCollection('query');
db.createCollection('blockly_document');
print("############# Start Insert Init Data##############")

db.trance_objects.insertMany(objects)
db.query.insertMany(queryObject)
print("############# End Insert Init Data##############")

db = db.getSiblingDB('api_test_db');
db.createUser(
    {
        user: 'trance_api_user',
        pwd: 'trance1234', // todo secure password
        roles: [{role: 'readWrite', db: 'api_test_db'}]
    }
);
db.createCollection('users');

print('END #################################');