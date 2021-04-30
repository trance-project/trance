
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
        name: "Sample",
        abr:"s",
        columns:
            [
                {name: "sample", children:{}},
                {name: "turmorsite", children:{}},
                {name: "treatment_outcome", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "CopyNumber",
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
        name: "Occurrences",
        abr:"o",
        columns:
            [
                {name: "sample", children:{}},
                {name: "contig", children:{}} ,
                {name: "end",  children:{}},
                {name: "reference", children:{}},
                {name: "alternate", children:{}},
                {name: "mutid", children:{}},
                {name: "candidates", children:
                    {name: "candidates",
                        columns:
                            [
                                {name: "gene", children:{}},
                                {name: "impact", children:{}},
                                {name: "sift", children:{}},
                                {name: "poly", children:{}},
                                {name: "consequences", children:
                                        {name: "consequences",
                                            columns:
                                                [
                                                    {name: "conseq", children: {}}
                                                ]
                                        }
                                    },
                            ]
                    }
                },
            ]
    }
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