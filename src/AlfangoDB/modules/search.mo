import Array "mo:base/Array";
import Bool "mo:base/Bool";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import Int "mo:base/Int";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Prelude "mo:base/Prelude";
import Text "mo:base/Text";
import Map "mo:map/Map";
import { thash } "mo:map/Map";

import Database "../types/database";
import Datatypes "../types/datatype";
import InputTypes "../types/input";
import OutputTypes "../types/output";
import SearchTypes "../types/search";
import Utils "../utils";

module {

    type AttributeDataValue = Datatypes.AttributeDataValue;
    type NumericAttributeDataValue = Datatypes.NumericAttributeDataValue;
    type StringAttributeDataValue = Datatypes.StringAttributeDataValue;

    type RelationalExpressionAttributeDataValue = SearchTypes.RelationalExpressionAttributeDataValue;
    type FilterExpressionConditionType = SearchTypes.FilterExpressionConditionType;
    type FilterExpressionType = SearchTypes.FilterExpressionType;
    type ContaintmentExpressionAttributeDataValue = SearchTypes.ContaintmentExpressionAttributeDataValue;

    public func scan({
        scanInput : InputTypes.ScanInputType;
        alfangoDB : Database.AlfangoDB;
    }) : OutputTypes.ScanOutputType {

        // get databases
        let databases = alfangoDB.databases;

        let { databaseName; tableName; filterExpressions } = scanInput;

        // check if database exists
        if (not Map.has(databases, thash, databaseName)) {
            let remark = "database does not exist: " # debug_show (databaseName);
            Debug.print(remark);
            return #err([remark]);
        };

        ignore do ? {
            let database = Map.get(databases, thash, databaseName)!;

            // check if table exists
            if (not Map.has(database.tables, thash, tableName)) {
                let remark = "table does not exist: " # debug_show (tableName);
                Debug.print(remark);
                return #err([remark]);
            };

            let table = Map.get(database.tables, thash, tableName)!;
            let tableItems = table.items;

            let filteredItemMap = Map.filter(
                tableItems,
                thash,
                func(_itemId : Database.Id, item : Database.Item) : Bool {
                    applyFilterExpression({ item; filterExpressions });
                },
            );

            let filteredItemsBuffer = Buffer.Buffer<{ id : Text; item : [(Text, Datatypes.AttributeDataValue)] }>(filteredItemMap.size());
            for (filteredItem in Map.valsDesc(filteredItemMap)) {
                filteredItemsBuffer.add({
                    id = filteredItem.id;
                    item = Map.toArray(filteredItem.attributeDataValueMap);
                });
            };

            return #ok(Buffer.toArray(filteredItemsBuffer));
        };

        Prelude.unreachable();
    };

    public func scanAndGetIds({
        scanAndGetIdsInput : InputTypes.ScanAndGetIdsInputType;
        alfangoDB : Database.AlfangoDB;
    }) : OutputTypes.ScanAndGetIdsOutputType {

        // get databases
        let databases = alfangoDB.databases;

        let { databaseName; tableName; filterExpressions } = scanAndGetIdsInput;

        // check if database exists
        if (not Map.has(databases, thash, databaseName)) {
            let remark = "database does not exist: " # debug_show (databaseName);
            Debug.print(remark);
            return #err([remark]);
        };

        ignore do ? {
            let database = Map.get(databases, thash, databaseName)!;

            // check if table exists
            if (not Map.has(database.tables, thash, tableName)) {
                let remark = "table does not exist: " # debug_show (tableName);
                Debug.print(remark);
                return #err([remark]);
            };

            let table = Map.get(database.tables, thash, tableName)!;
            let tableItems = table.items;

            let filteredItemIdsBuffer = Buffer.Buffer<Text>(0);
            for (item in Map.valsDesc(tableItems)) {
                // apply filter expression
                if (applyFilterExpression({ item; filterExpressions })) {
                    filteredItemIdsBuffer.add(item.id);
                };
            };

            return #ok({
                ids = Buffer.toArray(filteredItemIdsBuffer);
            });
        };

        Prelude.unreachable();
    };

    public func paginatedScan({
        paginatedScanInput : InputTypes.PaginatedScanInputType;
        alfangoDB : Database.AlfangoDB;
    }) : OutputTypes.PaginatedScanOutputType {

        // get databases
        let databases = alfangoDB.databases;

        let { databaseName; tableName; filterExpressions; limit; offset } = paginatedScanInput;

        // check if database exists
        if (not Map.has(databases, thash, databaseName)) {
            let remark = "database does not exist: " # debug_show (databaseName);
            Debug.print(remark);
            return #err([remark]);
        };

        ignore do ? {
            let database = Map.get(databases, thash, databaseName)!;

            // check if table exists
            if (not Map.has(database.tables, thash, tableName)) {
                let remark = "table does not exist: " # debug_show (tableName);
                Debug.print(remark);
                return #err([remark]);
            };

            let table = Map.get(database.tables, thash, tableName)!;
            let tableItems = table.items;

            // check if offser is out of bounds
            if (offset >= Map.size(tableItems)) {
                return #err(["offset is greater than the number of items in the table"]);
            };

            // check if limit is greater than 0
            if (limit == 0) {
                return #err(["limit should be greater than 0"]);
            };

            var itemIdx : Int = -1;
            var filteredItemCount : Nat = 0;
            let filteredItemBuffer = Buffer.Buffer<{ id : Text; item : [(Text, Datatypes.AttributeDataValue)] }>(limit);

            var items : [(Database.Id, Database.Item)] = [];

            var sortedItems : [(Database.Id, Database.Item)] = [];

            switch (paginatedScanInput.sortKey) {
                case (?sortKey) {
                    let sortDirection = Utils.initializeSortOrder(paginatedScanInput.sortDirection, #desc);

                    let sortKeyDataType = Utils.initializeSortKeyDataType(paginatedScanInput.sortKeyDataType, #nat);

                    switch (sortDirection) {
                        case (#desc) {
                            items := Map.toArrayDesc(tableItems);
                            sortedItems := Utils.sortDescending(items, sortKey, sortKeyDataType);
                        };
                        case (#asc) {
                            items := Map.toArray(tableItems);
                            sortedItems := Utils.sortAscending(items, sortKey, sortKeyDataType);
                        };
                    };

                };

                case null sortedItems := Map.toArrayDesc(tableItems);
            };

            let sortedItemsIter = Iter.fromArray(sortedItems);
            let sortedItemsMap = Map.fromIter<Database.Id, Database.Item>(sortedItemsIter, thash);

            label items for (item in Map.vals(sortedItemsMap)) {
                itemIdx := itemIdx + 1;
                Debug.print("itemIdx: " # debug_show (itemIdx) # " offset: " # debug_show (offset) # " limit: " # debug_show (limit) # " filteredItemCount: " # debug_show (filteredItemCount) # " item: " # debug_show (item.id));
                // apply filter expression
                if (applyFilterExpression({ item; filterExpressions })) {
                    filteredItemCount := filteredItemCount + 1;
                    // check if item is within the offset and limit
                    if (filteredItemCount > offset) {
                        if (filteredItemCount <= offset + limit) {
                            filteredItemBuffer.add({
                                id = item.id;
                                item = Map.toArray(item.attributeDataValueMap);
                            });
                            if (filteredItemCount == offset + limit) {
                                break items;
                            };
                        } else {
                            break items;
                        };
                    };
                };
            };

            return #ok({
                items = Buffer.toArray(filteredItemBuffer);
                offset;
                limit;
                scannedItemCount = itemIdx + 1;
                nonScannedItemCount = Map.size(tableItems) - (itemIdx + 1);
            });
        };

        Prelude.unreachable();
    };

    private func applyFilterExpression({
        item : Database.Item;
        filterExpressions : [FilterExpressionType];
    }) : Bool {

        let attributeDataValueMap = item.attributeDataValueMap;
        var filterExpressionResult = true;

        // iterate over filter expressions and apply them
        for (filterExpression in filterExpressions.vals()) {
            let { attributeName; filterExpressionCondition } = filterExpression;

            var currentFilterExpressionResult = false;
            // check if attribute exists
            if (Map.has(attributeDataValueMap, thash, attributeName)) {
                ignore do ? {
                    // apply filter expression condition when attribute exists
                    currentFilterExpressionResult := applyFilterExpressionCondition({
                        filterExpressionCondition;
                        attributeDataValue = Map.get(attributeDataValueMap, thash, attributeName)!;
                    });
                };
            } // if attribute does not exist, apply #NOT_EXISTS condition
            else if (filterExpressionCondition == #NOT_EXISTS) {
                currentFilterExpressionResult := true;
            };

            filterExpressionResult := filterExpressionResult and currentFilterExpressionResult;
        };

        return filterExpressionResult;
    };

    private func applyFilterExpressionCondition({
        filterExpressionCondition : FilterExpressionConditionType;
        attributeDataValue : AttributeDataValue;
    }) : Bool {

        switch (filterExpressionCondition) {
            case (#EQ(conditionAttributeDataValue)) {
                return applyFilterEQ({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#NEQ(conditionAttributeDataValue)) {
                return not applyFilterEQ({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#LT(conditionAttributeDataValue)) {
                return applyFilterLT({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#LTE(conditionAttributeDataValue)) {
                return applyFilterLTE({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#GT(conditionAttributeDataValue)) {
                return not applyFilterLTE({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#GTE(conditionAttributeDataValue)) {
                return not applyFilterLT({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#EXISTS) {
                return true;
            };
            case (#NOT_EXISTS) {
                return false;
            };
            case (#BEGINS_WITH(conditionAttributeDataValue)) {
                return applyFilterBEGINS_WITH({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#CONTAINS(conditionAttributeDataValue)) {
                return applyFilterCONTAINS({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#NOT_CONTAINS(conditionAttributeDataValue)) {
                return not applyFilterCONTAINS({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#IN(conditionAttributeDataValue)) {
                return applyFilterIN({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#BETWEEN(conditionAttributeDataValue)) {
                return applyFilterBETWEEN({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
            case (#NOT_BETWEEN(conditionAttributeDataValue)) {
                return not applyFilterBETWEEN({
                    attributeDataValue;
                    conditionAttributeDataValue;
                });
            };
        };

        return false;
    };

    private func applyFilterEQ({
        attributeDataValue : AttributeDataValue; // #list([#text("health"),#text(#"entertainment")])
        conditionAttributeDataValue : RelationalExpressionAttributeDataValue // #text("health");
    }) : Bool {
        Debug.print("Attribute data value --> " # debug_show (attributeDataValue));
        Debug.print("Condition data value --> " # debug_show (conditionAttributeDataValue));

        var areEqual = false;
        switch (conditionAttributeDataValue) {
            case (#int(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#int8(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int8(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#int16(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int16(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#int32(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int32(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#int64(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int64(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#nat(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#nat8(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat8(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#nat16(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat16(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#nat32(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat32(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#nat64(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat64(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#float(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#float(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#text(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#text(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (#list(attributeDataValue)) {
                        switch (attributeDataValue) {
                            case (attributeDataValue) {
                                label items for (atttibuteData in attributeDataValue.vals()) {
                                    switch (atttibuteData) {
                                        case (#text(atttibuteData)) {

                                            areEqual := inputDataValue == atttibuteData;

                                            if (areEqual) {
                                                break items;
                                            };
                                        };

                                        case _ areEqual := false;
                                    };

                                };
                            };

                        };

                    };

                    case (_) areEqual := false;
                };
            };
            case (#char(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#char(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#bool(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#bool(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#blob(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#blob(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
            case (#principal(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#principal(attributeDataValue)) areEqual := inputDataValue == attributeDataValue;
                    case (_) areEqual := false;
                };
            };
        };

        return areEqual;
    };

    private func applyFilterLT({
        attributeDataValue : AttributeDataValue;
        conditionAttributeDataValue : RelationalExpressionAttributeDataValue;
    }) : Bool {

        var isLessThan = false;
        switch (conditionAttributeDataValue) {
            case (#int(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#int8(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int8(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#int16(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int16(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#int32(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int32(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#int64(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int64(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#nat(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#nat8(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat8(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#nat16(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat16(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#nat32(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat32(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#nat64(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat64(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#float(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#float(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#text(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#text(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#char(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#char(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#bool(_inputDataValue)) {
                switch (attributeDataValue) {
                    case (#bool(attributeDataValue)) isLessThan := false;
                    case (_) isLessThan := false;
                };
            };
            case (#blob(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#blob(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
            case (#principal(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#principal(attributeDataValue)) isLessThan := attributeDataValue < inputDataValue;
                    case (_) isLessThan := false;
                };
            };
        };

        return isLessThan;
    };

    private func applyFilterLTE({
        attributeDataValue : AttributeDataValue;
        conditionAttributeDataValue : RelationalExpressionAttributeDataValue;
    }) : Bool {

        var isLessThanOrEqual = false;
        switch (conditionAttributeDataValue) {
            case (#int(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#int8(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int8(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#int16(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int16(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#int32(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int32(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#int64(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#int64(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#nat(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#nat8(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat8(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#nat16(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat16(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#nat32(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat32(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#nat64(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#nat64(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#float(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#float(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#text(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#text(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#char(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#char(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#bool(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#bool(attributeDataValue)) isLessThanOrEqual := attributeDataValue == inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#blob(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#blob(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
            case (#principal(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#principal(attributeDataValue)) isLessThanOrEqual := attributeDataValue <= inputDataValue;
                    case (_) isLessThanOrEqual := false;
                };
            };
        };

        return isLessThanOrEqual;
    };

    private func applyFilterBEGINS_WITH({
        attributeDataValue : AttributeDataValue;
        conditionAttributeDataValue : StringAttributeDataValue;
    }) : Bool {

        var beginsWith = false;
        switch (conditionAttributeDataValue) {
            case (#text(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#text(attributeDataValue)) beginsWith := Text.startsWith(attributeDataValue, #text inputDataValue);
                    case (_) beginsWith := false;
                };
            };
            case (#char(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#char(attributeDataValue)) beginsWith := attributeDataValue == inputDataValue;
                    case (_) beginsWith := false;
                };
            };
        };

        return beginsWith;
    };

    private func applyFilterCONTAINS({
        attributeDataValue : AttributeDataValue;
        conditionAttributeDataValue : ContaintmentExpressionAttributeDataValue;
    }) : Bool {

        var contains = false;
        switch (conditionAttributeDataValue) {
            case (#text(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#text(attributeDataValue)) contains := Text.contains(attributeDataValue, #text inputDataValue);
                    case (_) contains := false;
                };
            };
            case (#char(inputDataValue)) {
                switch (attributeDataValue) {
                    case (#char(attributeDataValue)) contains := attributeDataValue == inputDataValue;
                    case (_) contains := false;
                };
            };
            case (#list(inputDataValueList)) {
                switch (attributeDataValue) {
                    case (#list(attributeDataValueList)) {
                        return Array.foldLeft<RelationalExpressionAttributeDataValue, Bool>(
                            inputDataValueList,
                            true,
                            func(soFarContains, inputDataValue) = applyListFilter({
                                attributeDataValue = inputDataValue;
                                conditionAttributeDataValue = attributeDataValueList;
                            }),
                        );
                    };
                    case (_) contains := false;
                };
            };
        };

        return contains;
    };

    private func applyListFilter({
        attributeDataValue : AttributeDataValue;
        conditionAttributeDataValue : [RelationalExpressionAttributeDataValue];
    }) : Bool {
        var areEqual = false;

        label items for (conditionAttributeData in conditionAttributeDataValue.vals()) {

            areEqual := applyFilterEQ({
                attributeDataValue;
                conditionAttributeDataValue = conditionAttributeData;
            });

            if (areEqual) {
                break items;
            };

        };

        return areEqual;
    };

    private func applyFilterIN({
        attributeDataValue : AttributeDataValue; // [#text("health"),#text(#"entertainment")]
        conditionAttributeDataValue : [RelationalExpressionAttributeDataValue] // [#text("health")];
    }) : Bool {

        Debug.print("Attribute data value --> " # debug_show (attributeDataValue));
        Debug.print("Condition data value --> " # debug_show (conditionAttributeDataValue));

        return Array.find<RelationalExpressionAttributeDataValue>(
            conditionAttributeDataValue,
            func(conditionAttributeDataValueInValue : RelationalExpressionAttributeDataValue) : Bool {
                applyFilterEQ({
                    attributeDataValue;
                    conditionAttributeDataValue = conditionAttributeDataValueInValue;
                });
            },
        ) != null;
    };

    private func applyFilterBETWEEN({
        attributeDataValue : AttributeDataValue;
        conditionAttributeDataValue : (RelationalExpressionAttributeDataValue, RelationalExpressionAttributeDataValue);
    }) : Bool {

        var isBetween = false;
        switch (conditionAttributeDataValue) {
            case ((#int(lowerInputDataValue), #int(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#int(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#int8(lowerInputDataValue), #int8(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#int8(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#int16(lowerInputDataValue), #int16(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#int16(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#int32(lowerInputDataValue), #int32(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#int32(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#int64(lowerInputDataValue), #int64(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#int64(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#nat(lowerInputDataValue), #nat(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#nat(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#nat8(lowerInputDataValue), #nat8(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#nat8(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#nat16(lowerInputDataValue), #nat16(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#nat16(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#nat32(lowerInputDataValue), #nat32(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#nat32(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#nat64(lowerInputDataValue), #nat64(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#nat64(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#float(lowerInputDataValue), #float(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#float(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#text(lowerInputDataValue), #text(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#text(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case ((#char(lowerInputDataValue), #char(upperInputDataValue))) {
                switch (attributeDataValue) {
                    case (#char(attributeDataValue)) isBetween := lowerInputDataValue <= attributeDataValue and attributeDataValue <= upperInputDataValue;
                    case (_) isBetween := false;
                };
            };
            case _ {
                Prelude.unreachable();
            };
        };

        return isBetween;
    };

};
