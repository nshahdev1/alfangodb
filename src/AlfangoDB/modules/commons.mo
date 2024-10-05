import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import HashMap "mo:base/HashMap";
import Text "mo:base/Text";
import Map "mo:map/Map";
import { thash } "mo:map/Map";
import Set "mo:map/Set";

import Database "../types/database";
import Datatypes "../types/datatype";
import Utils "../utils";

module {

    type AttributeDataType = Datatypes.AttributeDataType;
    type AttributeDataValue = Datatypes.AttributeDataValue;
    type Item = Database.Item;
    type AttributeName = Database.AttributeName;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public func unwrapAttributeDataValue({
        attributeDataValue : AttributeDataValue;
    }) : (AttributeDataType) {

        var unwrappedAttributeDataType : AttributeDataType = #default;

        switch (attributeDataValue) {
            case (#int(_intValue)) { unwrappedAttributeDataType := #int };
            case (#int8(_int8Value)) { unwrappedAttributeDataType := #int8 };
            case (#int16(_int16Value)) { unwrappedAttributeDataType := #int16 };
            case (#int32(_int32Value)) { unwrappedAttributeDataType := #int32 };
            case (#int64(_int64Value)) { unwrappedAttributeDataType := #int64 };
            case (#nat(_natValue)) { unwrappedAttributeDataType := #nat };
            case (#nat8(_nat8Value)) { unwrappedAttributeDataType := #nat8 };
            case (#nat16(_nat16Value)) { unwrappedAttributeDataType := #nat16 };
            case (#nat32(_nat32Value)) { unwrappedAttributeDataType := #nat32 };
            case (#nat64(_nat64Value)) { unwrappedAttributeDataType := #nat64 };
            case (#float(_floatValue)) { unwrappedAttributeDataType := #float };
            case (#text(_textValue)) { unwrappedAttributeDataType := #text };
            case (#char(_charValue)) { unwrappedAttributeDataType := #char };
            case (#bool(_boolValue)) { unwrappedAttributeDataType := #bool };
            case (#principal(_principalValue)) {
                unwrappedAttributeDataType := #principal;
            };
            case (#blob(_blobValue)) { unwrappedAttributeDataType := #blob };
            case (#list(_listValue)) { unwrappedAttributeDataType := #list };
            case (#map(_mapValue)) { unwrappedAttributeDataType := #map };
            case (#default) { unwrappedAttributeDataType := #default };
        };

        return (unwrappedAttributeDataType);
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public func validateAttributeDataType({
        attributeDataValue : AttributeDataValue;
        expectedAttributeDataType : AttributeDataType;
    }) : {
        isValidAttributeDataType : Bool;
        actualAttributeDataType : AttributeDataType;
    } {

        let actualAttributeDataType = unwrapAttributeDataValue({
            attributeDataValue;
        });

        return {
            isValidAttributeDataType = (actualAttributeDataType == expectedAttributeDataType);
            actualAttributeDataType;
        };
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public func validateAttributeDataTypes({
        attributeKeyDataValues : [(Text, AttributeDataValue)];
        attributeNameToMetadataMap : Map.Map<Text, Database.AttributeMetadata>;
    }) : {
        isValidAttributesDataType : Bool;
    } {

        let unwantedAttributes = Buffer.Buffer<Text>(0);
        let invalidAttributes = Buffer.Buffer<Text>(0);

        for ((attributeName, attributeDataValue) in attributeKeyDataValues.vals()) {
            let attributeExistInTable = Map.has(attributeNameToMetadataMap, thash, attributeName);

            if (attributeExistInTable) {
                var expectedAttributeDataType : Datatypes.AttributeDataType = #default;
                ignore do ? {
                    expectedAttributeDataType := Map.get(attributeNameToMetadataMap, thash, attributeName)!.dataType;
                };
                let {
                    isValidAttributeDataType;
                    actualAttributeDataType;
                } = validateAttributeDataType({
                    attributeDataValue;
                    expectedAttributeDataType;
                });

                if (not isValidAttributeDataType) {
                    invalidAttributes.add(attributeName);
                    Debug.print("attribute: " # debug_show (attributeName) # " has invalid data type: " # debug_show (actualAttributeDataType));
                };
            } else {
                unwantedAttributes.add(attributeName);
                Debug.print("attribute: " # debug_show (attributeName) # " is not in table");
            };
        };

        return {
            isValidAttributesDataType = invalidAttributes.size() == 0 and unwantedAttributes.size() == 0;
        };
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public func validateUniqueAttribute({
        attributeKeyDataValue : (AttributeName, AttributeDataValue);
        indexTable : Database.IndexTable;
    }) : Bool {

        let (attributeName, attributeDataValue) = attributeKeyDataValue;
        let indexItems = indexTable.items;

        var isValidUniqueAttribute = true;
        ignore do ? {
            let idSet = Map.get(indexItems, Utils.DataTypeValueHashUtils, attributeDataValue)!;
            if (Set.size(idSet) > 0) {
                isValidUniqueAttribute := false;
                Debug.print("attribute: " # debug_show (attributeKeyDataValue) # " is not unique");
            };
        };

        return isValidUniqueAttribute;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public func validateUniqueAttributes({
        attributeKeyDataValues : [(Text, Datatypes.AttributeDataValue)];
        indexes : Map.Map<Text, Database.IndexTable>;
        tableMetadata : Database.TableMetadata;
    }) : {
        invalidUnquieAttributes : [Text];
        uniqueAttributesUnique : Bool;
    } {

        let attributeDataValueMap = HashMap.fromIter<Text, Datatypes.AttributeDataValue>(attributeKeyDataValues.vals(), 0, Text.equal, Text.hash);
        let invalidUnquieAttributes = Buffer.Buffer<Text>(0);

        label l0 for (attributeMetadata in Map.vals(tableMetadata.attributesMap)) {
            // check for unique attribute
            if (not attributeMetadata.unique) {
                continue l0;
            };

            let attributeName = attributeMetadata.name;
            ignore do ? {
                let attributeValue = attributeDataValueMap.get(attributeName)!;
                let isValidUniqueAttribute = validateUniqueAttribute({
                    attributeKeyDataValue = (attributeName, attributeValue);
                    indexTable = Map.get(indexes, thash, attributeName)!;
                });
                if (not isValidUniqueAttribute) {
                    invalidUnquieAttributes.add(attributeName);
                };
            };
        };

        return {
            invalidUnquieAttributes = Buffer.toArray(invalidUnquieAttributes);
            uniqueAttributesUnique = invalidUnquieAttributes.size() == 0;
        };
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

};
