import InputTypes "../types/input";
import Datatypes "../types/datatype";
import Database "../types/database";
import Map "mo:map/Map";
import { thash } "mo:map/Map";
import Debug "mo:base/Debug";

module {

    public func getItemById({
        getItemByIdInput: InputTypes.GetItemByIdInputType;
        alfangoDB: Database.AlfangoDB;
    }) : ?[ (Text, Datatypes.AttributeDataValue) ] {

        // get databases
        let databases = alfangoDB.databases;

        // check if database exists
        if (not Map.has(databases, thash, getItemByIdInput.databaseName)) {
            Debug.print("database does not exist");
            return null;
        };

        do ?{
            let database = Map.get(databases, thash, getItemByIdInput.databaseName)!;

            // check if table exists
            if (not Map.has(database.tables, thash, getItemByIdInput.tableName)) {
                Debug.print("table does not exist");
                return null;
            };

            let table = Map.get(database.tables, thash, getItemByIdInput.tableName)!;
            // check if item exists
            if (not Map.has(table.items, thash, getItemByIdInput.id)) {
                Debug.print("item does not exist");
                return null;
            };

            // get item
            let item = Map.get(table.items, thash, getItemByIdInput.id)!;
            return ?Map.toArray(item.attributeDataValueMap);
        };
    };

};