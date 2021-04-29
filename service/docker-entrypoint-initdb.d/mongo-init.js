/*
    This is the init data when you would like to reset db state it should revert to this state
 */

/////////TRANCE_OBJECTS////////////////
const objects = [
    {
        _id: "72e52011-5523-44d8-885b-b3677321aa62",
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
        _id: "1f03516d-51f0-4cd1-840c-d78115edab63",
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
        _id: "1f03516d-51f0-4cd1-840c-d78115dbab67",
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
                        _id: "1f03516d-51f0-4cd1-840c-d78115dbdb57",
                        columns:
                            [
                                {name: "gene", children:{}},
                                {name: "impact", children:{}},
                                {name: "sift", children:{}},
                                {name: "poly", children:{}},
                                {name: "consequences", children:
                                        {name: "consequences",
                                            _id: "1f03573c-51f1-4cd1-840c-d78115dbcc66",
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
     _id: "1c3d54f8-a876-418e-adaa-fb997e0f1405",
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