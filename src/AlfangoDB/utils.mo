import Array "mo:base/Array";
import Bool "mo:base/Bool";
import Buffer "mo:base/Buffer";
import Char "mo:base/Char";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Int16 "mo:base/Int16";
import Int32 "mo:base/Int32";
import Int64 "mo:base/Int64";
import Int8 "mo:base/Int8";
import Nat "mo:base/Nat";
import Nat16 "mo:base/Nat16";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Nat8 "mo:base/Nat8";
import Order "mo:base/Order";
import Prelude "mo:base/Prelude";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Map "mo:map/Map";
import {
    bhash;
    i16hash;
    i32hash;
    i64hash;
    i8hash;
    ihash;
    lhash;
    n16hash;
    n32hash;
    n64hash;
    n8hash;
    nhash;
    phash;
    thash;
} "mo:map/Map";
import XorShift "mo:rand/XorShift";
import ULIDAsyncSource "mo:ulid/async/Source";
import ULIDSource "mo:ulid/Source";
import ULID "mo:ulid/ULID";

import InputTypes "/types/input";
import Database "types/database";
import Datatypes "types/datatype";
import Datatype "types/datatype";
import Output "types/output";

module {

    type AttributeDataType = Datatypes.AttributeDataType;
    type AttributeDataValue = Datatypes.AttributeDataValue;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private func getHash(attributeDataValue : AttributeDataValue) : Nat32 {

        var hash : Nat32 = 0;

        switch (attributeDataValue) {
            case (#text(value)) hash := thash.0 (value);
            case (#int(value)) hash := ihash.0 (value);
            case (#int8(value)) hash := i8hash.0 (value);
            case (#int16(value)) hash := i16hash.0 (value);
            case (#int32(value)) hash := i32hash.0 (value);
            case (#int64(value)) hash := i64hash.0 (value);
            case (#nat(value)) hash := nhash.0 (value);
            case (#nat8(value)) hash := n8hash.0 (value);
            case (#nat16(value)) hash := n16hash.0 (value);
            case (#nat32(value)) hash := n32hash.0 (value);
            case (#nat64(value)) hash := n64hash.0 (value);
            case (#blob(value)) hash := bhash.0 (value);
            case (#bool(value)) hash := lhash.0 (value);
            case (#principal(value)) hash := phash.0 (value);
            case (_) Prelude.nyi();
        };

        return hash;
    };

    private func areEqual(attributeDataValue1 : AttributeDataValue, attributeDataValue2 : AttributeDataValue) : Bool {

        var areEqual : Bool = false;

        switch (attributeDataValue1) {
            case (#text(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#text(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#int(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#int(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#int8(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#int8(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#int16(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#int16(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#int32(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#int32(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#int64(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#int64(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#nat(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#nat(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#nat8(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#nat8(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#nat16(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#nat16(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#nat32(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#nat32(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#nat64(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#nat64(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#blob(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#blob(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#bool(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#bool(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (#principal(attributeValue1)) {
                switch (attributeDataValue2) {
                    case (#principal(attributeValue2)) areEqual := attributeValue1 == attributeValue2;
                    case (_) Prelude.unreachable();
                };
            };
            case (_) {
                Prelude.unreachable();
            };
        };

        return areEqual;
    };

    public let DataTypeValueHashUtils : Map.HashUtils<AttributeDataValue> = (getHash, areEqual);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public func generateULIDSync() : Text {

        ULID.toText(ULIDSource.Source(XorShift.toReader(XorShift.XorShift64(null)), 123).new());
    };

    public func generateULIDAsync() : async Text {

        ULID.toText(await ULIDAsyncSource.Source(0).new());
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public func getSortOrder(
        sortDirection : Datatypes.SortDirection,
        sortKeyDataType : Datatypes.AttributeDataType,
        sortKey : Text,
        events : [Output.ItemOutputType],
    ) : [Output.ItemOutputType] {

        var sortedItems : [Output.ItemOutputType] = [];

        switch (sortKeyDataType) {
            case (#nat) {

                sortedItems := Array.sort(
                    events,
                    (
                        func(a : Output.ItemOutputType, b : Output.ItemOutputType) : Order.Order {
                            let array_a = a.item;
                            let key_a = getNatKeyValue(array_a, sortKey);

                            let array_b = b.item;
                            let key_b = getNatKeyValue(array_b, sortKey);

                            return handleNatComparsion(key_a, key_b, sortDirection);
                        }
                    ),
                );
            };
            case (#nat64) {

                sortedItems := Array.sort(
                    events,
                    (
                        func(a : Output.ItemOutputType, b : Output.ItemOutputType) : Order.Order {
                            let array_a = a.item;
                            let key_a = getNat64KeyValue(array_a, sortKey);

                            let array_b = b.item;
                            let key_b = getNat64KeyValue(array_b, sortKey);

                            return handleNat64Comparsion(key_a, key_b, sortDirection);
                        }
                    ),
                );
            };
            case _ {
                sortedItems := Array.sort(
                    events,
                    (
                        func(a : Output.ItemOutputType, b : Output.ItemOutputType) : Order.Order {
                            let array_a = a.item;
                            let key_a = getTextKeyValue(array_a, sortKey);

                            let array_b = b.item;
                            let key_b = getTextKeyValue(array_b, sortKey);

                            return handleTextComparision(key_a, key_b, sortDirection);
                        }
                    ),
                );
            };

        };
        return sortedItems;
    };

    public func sortItems(
        sortObject : InputTypes.SortMultipleInputType,
        events : [Output.ItemOutputType],
    ) : [Output.ItemOutputType] {

        var sortedItems : [Output.ItemOutputType] = [];

        sortedItems := Array.sort(
            events,
            (
                func(a : Output.ItemOutputType, b : Output.ItemOutputType) : Order.Order {

                    var comparison : Order.Order = #equal;

                    ignore do ? {
                        label sortKeyArrayItems for (sortKeyObject in sortObject.vals()) {

                            let sortDirection = sortKeyObject.sortDirection!;
                            let sortKeyDataType = sortKeyObject.sortKeyDataType!;
                            let sortKey = sortKeyObject.sortKey!;

                            switch (sortKeyDataType) {
                                case (#nat) {

                                    let array_a = a.item;
                                    let key_a = getNatKeyValue(array_a, sortKey);

                                    let array_b = b.item;
                                    let key_b = getNatKeyValue(array_b, sortKey);

                                    comparison := handleNatComparsion(key_a, key_b, sortDirection);
                                };
                                case (#nat64) {

                                    let array_a = a.item;
                                    let key_a = getNat64KeyValue(array_a, sortKey);

                                    let array_b = b.item;
                                    let key_b = getNat64KeyValue(array_b, sortKey);

                                    comparison := handleNat64Comparsion(key_a, key_b, sortDirection);

                                };

                                case (#bool) {

                                    let array_a = a.item;
                                    let key_a = getBoolKeyValue(array_a, sortKey);

                                    let array_b = b.item;
                                    let key_b = getBoolKeyValue(array_b, sortKey);

                                    comparison := handleBoolComparsion(key_a, key_b, sortDirection);
                                };

                                case _ {
                                    let array_a = a.item;
                                    let key_a = getTextKeyValue(array_a, sortKey);

                                    let array_b = b.item;
                                    let key_b = getTextKeyValue(array_b, sortKey);

                                    comparison := handleTextComparision(key_a, key_b, sortDirection);

                                };
                            };

                            if (comparison != #equal) {
                                break sortKeyArrayItems;
                            };
                        };
                    };
                    return comparison;
                }
            ),
        );

        return sortedItems;
    };

    public func getBoolKeyValue(array : [(Database.AttributeName, Datatypes.AttributeDataValue)], sortKey : Text) : Bool {

        let tuple = Array.find<(Text, Datatypes.AttributeDataValue)>(
            array,
            func(tuple) : Bool {
                return tuple.0 == sortKey;
            },
        );

        switch (tuple) {
            case null false;

            case (?result) {
                do {
                    let (_, attributeDataValue) = result;
                    let value = getAttributeDataValue({ attributeDataValue });
                    return textToBool(value);
                };
            };
        };

    };

    public func getNatKeyValue(array : [(Database.AttributeName, Datatypes.AttributeDataValue)], sortKey : Text) : Nat {

        let tuple = Array.find<(Text, Datatypes.AttributeDataValue)>(
            array,
            func(tuple) : Bool {
                return tuple.0 == sortKey;
            },
        );

        switch (tuple) {
            case null 0;

            case (?result) {
                do {
                    let (_, attributeDataValue) = result;
                    let value = getAttributeDataValue({ attributeDataValue });
                    return textToNat(value);
                };
            };
        };

    };

    public func getNat64KeyValue(array : [(Database.AttributeName, Datatypes.AttributeDataValue)], sortKey : Text) : Nat64 {

        let tuple = Array.find<(Text, Datatypes.AttributeDataValue)>(
            array,
            func(tuple) : Bool {
                return tuple.0 == sortKey;
            },
        );

        switch (tuple) {
            case null 0;

            case (?result) {
                do {
                    let (_, attributeDataValue) = result;
                    let value = getAttributeDataValue({ attributeDataValue });
                    return textToNat64(value);
                };
            };
        };

    };

    public func getTextKeyValue(array : [(Database.AttributeName, Datatypes.AttributeDataValue)], sortKey : Text) : Text {

        let tuple = Array.find<(Text, Datatypes.AttributeDataValue)>(
            array,
            func(tuple) : Bool {
                return tuple.0 == sortKey;
            },
        );

        switch (tuple) {
            case null "";

            case (?result) {
                do {
                    let (_, attributeDataValue) = result;
                    let value = getAttributeDataValue({ attributeDataValue });
                    return value;
                };
            };
        };

    };

    public func textToNat(txt : Text) : Nat {
        if (txt.size() > 0) {
            let chars = txt.chars();

            var num : Nat = 0;
            for (v in chars) {
                let charToNum = Nat32.toNat(Char.toNat32(v) -48);
                assert (charToNum >= 0 and charToNum <= 9);
                num := num * 10 + charToNum;
            };

            num;
        } else {
            0;
        };
    };

    public func textToNat64(t : Text) : Nat64 {
        Nat64.fromNat(textToNat(t));
    };

    public func textToBool(text : Text) : Bool {
        switch (text) {
            case "true" { return true };
            case "false" { return false };
            case _ { return false }; // default case
        };
    };

    public func getAttributeDataValue({
        attributeDataValue : AttributeDataValue;
    }) : Text {

        switch (attributeDataValue) {
            case (#int(intValue)) { Int.toText(intValue) };
            case (#int8(int8Value)) { Int8.toText(int8Value) };
            case (#int16(int16Value)) { Int16.toText(int16Value) };
            case (#int32(int32Value)) { Int32.toText(int32Value) };
            case (#int64(int64Value)) { Int64.toText(int64Value) };
            case (#nat(natValue)) { Nat.toText(natValue) };
            case (#nat8(nat8Value)) { Nat8.toText(nat8Value) };
            case (#nat16(nat16Value)) { Nat16.toText(nat16Value) };
            case (#nat32(nat32Value)) { Nat32.toText(nat32Value) };
            case (#nat64(nat64Value)) { Nat64.toText(nat64Value) };
            case (#float(floatValue)) { Float.toText(floatValue) };
            case (#text(textValue)) { textValue };
            case (#char(charValue)) { Char.toText(charValue) };
            case (#bool(boolValue)) { Bool.toText(boolValue) };
            case (#principal(principalValue)) {
                Principal.toText(principalValue);
            };
            case (#blob(blobValue)) {
                switch (Text.decodeUtf8(blobValue)) {
                    case null "";
                    case (?blobText) { blobText };
                };
            };
            case (#list(listValue)) {
                let buffer = Buffer.Buffer<Text>(0);
                for (value in listValue.vals()) {
                    switch (value) {
                        case (#text(textValue)) { buffer.add(textValue) };
                        case (#int(intValue)) {
                            buffer.add(Int.toText(intValue));
                        };
                        case (#int8(int8Value)) {
                            buffer.add(Int8.toText(int8Value));
                        };
                        case (#int16(int16Value)) {
                            buffer.add(Int16.toText(int16Value));
                        };
                        case (#int32(int32Value)) {
                            buffer.add(Int32.toText(int32Value));
                        };
                        case (#int64(int64Value)) {
                            buffer.add(Int64.toText(int64Value));
                        };
                        case (#nat(natValue)) {
                            buffer.add(Nat.toText(natValue));
                        };
                        case (#nat8(nat8Value)) {
                            buffer.add(Nat8.toText(nat8Value));
                        };
                        case (#nat16(nat16Value)) {
                            buffer.add(Nat16.toText(nat16Value));
                        };
                        case (#nat32(nat32Value)) {
                            buffer.add(Nat32.toText(nat32Value));
                        };
                        case (#nat64(nat64Value)) {
                            buffer.add(Nat64.toText(nat64Value));
                        };
                        case (#float(floatValue)) {
                            buffer.add(Float.toText(floatValue));
                        };
                        case (#char(charValue)) {
                            buffer.add(Char.toText(charValue));
                        };
                    };
                };
                return textArrayToString(Buffer.toArray(buffer));
            };
            case (#map(mapValue)) {
                let buffer = Buffer.Buffer<(Text, Text)>(0);
                for ((key, value) in mapValue.vals()) {
                    switch (value) {
                        case (#text(textValue)) { buffer.add((key, textValue)) };
                        case (#int(intValue)) {
                            buffer.add((key, Int.toText(intValue)));
                        };
                        case (#int8(int8Value)) {
                            buffer.add((key, Int8.toText(int8Value)));
                        };
                        case (#int16(int16Value)) {
                            buffer.add((key, Int16.toText(int16Value)));
                        };
                        case (#int32(int32Value)) {
                            buffer.add((key, Int32.toText(int32Value)));
                        };
                        case (#int64(int64Value)) {
                            buffer.add((key, Int64.toText(int64Value)));
                        };
                        case (#nat(natValue)) {
                            buffer.add((key, Nat.toText(natValue)));
                        };
                        case (#nat8(nat8Value)) {
                            buffer.add((key, Nat8.toText(nat8Value)));
                        };
                        case (#nat16(nat16Value)) {
                            buffer.add((key, Nat16.toText(nat16Value)));
                        };
                        case (#nat32(nat32Value)) {
                            buffer.add((key, Nat32.toText(nat32Value)));
                        };
                        case (#nat64(nat64Value)) {
                            buffer.add((key, Nat64.toText(nat64Value)));
                        };
                        case (#float(floatValue)) {
                            buffer.add((key, Float.toText(floatValue)));
                        };
                        case (#char(charValue)) {
                            buffer.add((key, Char.toText(charValue)));
                        };
                        case (#list(listValue)) {
                            let buffer = Buffer.Buffer<Text>(0);
                            for (value in listValue.vals()) {
                                switch (value) {
                                    case (#text(textValue)) {
                                        buffer.add(textValue);
                                    };
                                    case (#int(intValue)) {
                                        buffer.add(Int.toText(intValue));
                                    };
                                    case (#int8(int8Value)) {
                                        buffer.add(Int8.toText(int8Value));
                                    };
                                    case (#int16(int16Value)) {
                                        buffer.add(Int16.toText(int16Value));
                                    };
                                    case (#int32(int32Value)) {
                                        buffer.add(Int32.toText(int32Value));
                                    };
                                    case (#int64(int64Value)) {
                                        buffer.add(Int64.toText(int64Value));
                                    };
                                    case (#nat(natValue)) {
                                        buffer.add(Nat.toText(natValue));
                                    };
                                    case (#nat8(nat8Value)) {
                                        buffer.add(Nat8.toText(nat8Value));
                                    };
                                    case (#nat16(nat16Value)) {
                                        buffer.add(Nat16.toText(nat16Value));
                                    };
                                    case (#nat32(nat32Value)) {
                                        buffer.add(Nat32.toText(nat32Value));
                                    };
                                    case (#nat64(nat64Value)) {
                                        buffer.add(Nat64.toText(nat64Value));
                                    };
                                    case (#float(floatValue)) {
                                        buffer.add(Float.toText(floatValue));
                                    };
                                    case (#char(charValue)) {
                                        buffer.add(Char.toText(charValue));
                                    };
                                };
                            };
                            return textArrayToString(Buffer.toArray(buffer));
                        };
                    };
                };
                return arrayTupleToString(Buffer.toArray(buffer));
            };
            case (#default) "";
        };

    };

    public func textArrayToString(arr : [Text]) : Text {
        var res : Text = "";

        for (i in arr.keys()) {
            let val = arr[i];

            if (i > 0) {
                res := res # "," # val;
            } else {
                res := val;
            };

        };
        return res;
    };

    public func arrayTupleToString(arr : [(Text, Text)]) : Text {
        var res : Text = "";

        for (i in arr.keys()) {
            let val = arr[i];

            let (keyPart, valuePart) = val;
            if (i > 0) {
                res := res # "," # "(" # keyPart # "," # valuePart # ")";
            } else {
                res := "(" # keyPart # "," # valuePart # ")";
            };

        };
        return res;
    };

    public func sortAscending(items : [(Database.Id, Database.Item)], sortKey : Text, sortKeyDataType : Datatypes.AttributeDataType) : [(Database.Id, Database.Item)] {
        var sortedItems : [(Database.Id, Database.Item)] = [];

        switch (sortKeyDataType) {
            case (#nat) {
                sortedItems := Array.sort(
                    items,
                    (
                        func(a : (Database.Id, Database.Item), b : (Database.Id, Database.Item)) : Order.Order {
                            let array_a = Map.toArray(a.1.attributeDataValueMap);

                            let array_b = Map.toArray(b.1.attributeDataValueMap);

                            return Nat.compare(getNatKeyValue(array_a, sortKey), getNatKeyValue(array_b, sortKey)); // Sort in ascending order
                        }
                    ),
                );
            };
            case (#nat64) {
                sortedItems := Array.sort(
                    items,
                    (
                        func(a : (Database.Id, Database.Item), b : (Database.Id, Database.Item)) : Order.Order {
                            let array_a = Map.toArray(a.1.attributeDataValueMap);

                            let array_b = Map.toArray(b.1.attributeDataValueMap);

                            return Nat64.compare(getNat64KeyValue(array_a, sortKey), getNat64KeyValue(array_b, sortKey)); // Sort in descending order
                        }
                    ),
                );
            };
            case _ {
                sortedItems := Array.sort(
                    items,
                    (
                        func(a : (Database.Id, Database.Item), b : (Database.Id, Database.Item)) : Order.Order {
                            let array_a = Map.toArray(a.1.attributeDataValueMap);

                            let array_b = Map.toArray(b.1.attributeDataValueMap);

                            return Text.compare(getTextKeyValue(array_a, sortKey), getTextKeyValue(array_b, sortKey)); // Sort in ascending order
                        }
                    ),
                );
            };
        };
        return sortedItems;
    };

    public func sortMap(sortObject : InputTypes.SortMultipleInputType, items : [(Database.Id, Database.Item)]) : [(Database.Id, Database.Item)] {
        var sortedItems : [(Database.Id, Database.Item)] = [];

        sortedItems := Array.sort(
            items,
            (
                func(a : (Database.Id, Database.Item), b : (Database.Id, Database.Item)) : Order.Order {

                    var comparison : Order.Order = #equal;

                    ignore do ? {
                        label sortKeyMapItems for (sortKeyObject in sortObject.vals()) {

                            let sortDirection = sortKeyObject.sortDirection!;
                            let sortKeyDataType = sortKeyObject.sortKeyDataType!;
                            let sortKey = sortKeyObject.sortKey!;

                            switch (sortKeyDataType) {
                                case (#nat) {
                                    let array_a = Map.toArray(a.1.attributeDataValueMap);
                                    let key_a = getNatKeyValue(array_a, sortKey);

                                    let array_b = Map.toArray(b.1.attributeDataValueMap);
                                    let key_b = getNatKeyValue(array_b, sortKey);

                                    comparison := handleNatComparsion(key_a, key_b, sortDirection);

                                };
                                case (#nat64) {

                                    let array_a = Map.toArray(a.1.attributeDataValueMap);
                                    let key_a = getNat64KeyValue(array_a, sortKey);

                                    let array_b = Map.toArray(b.1.attributeDataValueMap);
                                    let key_b = getNat64KeyValue(array_b, sortKey);

                                    comparison := handleNat64Comparsion(key_a, key_b, sortDirection);

                                };
                                case _ {

                                    let array_a = Map.toArray(a.1.attributeDataValueMap);
                                    let key_a = getTextKeyValue(array_a, sortKey);

                                    let array_b = Map.toArray(b.1.attributeDataValueMap);
                                    let key_b = getTextKeyValue(array_b, sortKey);

                                    comparison := handleTextComparision(key_a, key_b, sortDirection);

                                };
                            };

                            if (comparison != #equal) {
                                break sortKeyMapItems;
                            };
                        };

                    };

                    return comparison;
                }
            ),
        );

        return sortedItems;
    };

    public func sortMultipleItems(
        items : [(Database.Id, Database.Item)],
        sortKeysArray : InputTypes.SortMultipleInputType,
    ) : [(Database.Id, Database.Item)] {
        var sortedItems : [(Database.Id, Database.Item)] = [];

        sortedItems := Array.sort(
            items,
            (
                func(a : (Database.Id, Database.Item), b : (Database.Id, Database.Item)) : Order.Order {

                    var comparison : Order.Order = #equal;

                    ignore do ? {

                        label sortKeyArrayItems for (sortKeyObject in sortKeysArray.vals()) {

                            let sortDirection = sortKeyObject.sortDirection!;
                            let sortKeyDataType = sortKeyObject.sortKeyDataType!;
                            let sortKey = sortKeyObject.sortKey!;

                            switch (sortKeyDataType) {

                                case (#nat) {
                                    let array_a = Map.toArray(a.1.attributeDataValueMap);
                                    let key_a = getNatKeyValue(array_a, sortKey);

                                    let array_b = Map.toArray(b.1.attributeDataValueMap);
                                    let key_b = getNatKeyValue(array_b, sortKey);

                                    comparison := handleNatComparsion(key_a, key_b, sortDirection);
                                };

                                case (#nat64) {
                                    let array_a = Map.toArray(a.1.attributeDataValueMap);
                                    let key_a = getNat64KeyValue(array_a, sortKey);

                                    let array_b = Map.toArray(b.1.attributeDataValueMap);
                                    let key_b = getNat64KeyValue(array_b, sortKey);

                                    comparison := handleNat64Comparsion(key_a, key_b, sortDirection);

                                };

                                case (#bool) {

                                    let array_a = Map.toArray(a.1.attributeDataValueMap);
                                    let key_a = getBoolKeyValue(array_a, sortKey);

                                    let array_b = Map.toArray(b.1.attributeDataValueMap);
                                    let key_b = getBoolKeyValue(array_b, sortKey);

                                    comparison := handleBoolComparsion(key_a, key_b, sortDirection);
                                };

                                case _ {

                                    let array_a = Map.toArray(a.1.attributeDataValueMap);
                                    let key_a = getTextKeyValue(array_a, sortKey);

                                    let array_b = Map.toArray(b.1.attributeDataValueMap);
                                    let key_b = getTextKeyValue(array_b, sortKey);

                                    comparison := handleTextComparision(key_a, key_b, sortDirection);

                                };
                            };

                            if (comparison != #equal) {
                                break sortKeyArrayItems;
                            };
                        };

                    };
                    return comparison;
                }
            ),

        );

        return sortedItems;
    };

    public func handleNatComparsion(key_a : Nat, key_b : Nat, sortDirection : Datatypes.SortDirection) : Order.Order {

        var comparison = Nat.compare(key_a, key_b);

        if (comparison != #equal) {
            switch (sortDirection) {
                case (#asc) comparison := comparison;
                case (#desc) {
                    if (comparison == #less) {
                        comparison := #greater;
                    } else {
                        comparison := #less;
                    };
                };
            };

        };

        return comparison;
    };

    public func handleBoolComparsion(key_a : Bool, key_b : Bool, sortDirection : Datatypes.SortDirection) : Order.Order {

        var comparison = Bool.compare(key_a, key_b);

        if (comparison != #equal) {
            switch (sortDirection) {
                case (#asc) comparison := comparison;
                case (#desc) {
                    if (comparison == #less) {
                        comparison := #greater;
                    } else {
                        comparison := #less;
                    };
                };
            };

        };

        return comparison;
    };

    public func handleNat64Comparsion(key_a : Nat64, key_b : Nat64, sortDirection : Datatypes.SortDirection) : Order.Order {

        var comparison = Nat64.compare(key_a, key_b);

        if (comparison != #equal) {
            switch (sortDirection) {
                case (#asc) comparison := comparison;
                case (#desc) {
                    if (comparison == #less) {
                        comparison := #greater;
                    } else {
                        comparison := #less;
                    };
                };
            };

        };

        return comparison;
    };

    public func handleTextComparision(key_a : Text, key_b : Text, sortDirection : Datatypes.SortDirection) : Order.Order {

        var comparison = Text.compare(key_a, key_b);

        if (comparison != #equal) {
            switch (sortDirection) {
                case (#asc) comparison := comparison;
                case (#desc) {
                    if (comparison == #less) {
                        comparison := #greater;
                    } else {
                        comparison := #less;
                    };
                };
            };

        };

        return comparison;
    };

    public func initializeSortObject(payload : ?InputTypes.SortInputType) : InputTypes.SortInputType {

        switch (payload) {
            case (?sortObject) {
                return {
                    sortKey = sortObject.sortKey;
                    sortKeyDataType = sortObject.sortKeyDataType;
                    sortDirection = sortObject.sortDirection;
                };
            };
            case null {
                return {
                    sortKey = null;
                    sortKeyDataType = ? #nat;
                    sortDirection = ? #desc;
                };
            };
        };

    };

    public func initializeSortObjectArray(payload : ?InputTypes.SortMultipleInputType) : InputTypes.SortMultipleInputType {

        switch (payload) {
            case (?sortObjectArr) {
                return sortObjectArr;
            };
            case null {
                return [{
                    sortKey = null;
                    sortKeyDataType = ? #nat;
                    sortDirection = ? #desc;
                }];
            };
        };

    };

    public func initializeSearchObject(payload : ?InputTypes.SearchInputType) : InputTypes.SearchInputType {

        switch (payload) {
            case (?searchObject) {
                return {
                    searchValue = searchObject.searchValue;
                    foreignKeys = searchObject.foreignKeys;

                };
            };
            case null {
                return {
                    searchValue = null;
                    foreignKeys = ?[];
                };
            };
        };

    };

    public func initializeTextField(payload : ?Text, initialValue : Text) : Text {
        switch (payload) {
            case (?fieldValue) fieldValue;
            case null initialValue;
        };
    };

    public func initializeTextArrayField(payload : ?[Text], initialValue : [Text]) : [Text] {
        switch (payload) {
            case (?fieldValue) fieldValue;
            case null initialValue;
        };
    };

    public func initializeSortOrder(payload : ?Datatype.SortDirection, initialValue : Datatype.SortDirection) : Datatype.SortDirection {
        switch (payload) {
            case (?fieldValue) fieldValue;
            case null initialValue;
        };
    };

    public func initializeSortKeyDataType(payload : ?Datatype.AttributeDataType, initialValue : Datatype.AttributeDataType) : Datatype.AttributeDataType {
        switch (payload) {
            case (?fieldValue) fieldValue;
            case null initialValue;
        };
    };
};
