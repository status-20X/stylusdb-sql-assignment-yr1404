const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

// Helper functions for different JOIN types
function performInnerJoin(mainData, joinData, joinCondition, fields, mainTable) {
    return mainData.flatMap(mainRow => {
        return joinData
            .filter(joinRow => mainRow[joinCondition.left.split('.')[1]] === joinRow[joinCondition.right.split('.')[1]])
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === mainTable ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });
}

function performLeftJoin(mainData, joinData, joinCondition, fields, mainTable) {
    return mainData.flatMap(mainRow => {
        const matchingJoinRows = joinData.filter(joinRow => mainRow[joinCondition.left.split('.')[1]] === joinRow[joinCondition.right.split('.')[1]]);
        if (matchingJoinRows.length === 0) {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === mainTable ? mainRow[fieldName] : null;
                return acc;
            }, {});
        }
        return matchingJoinRows.map(joinRow => {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === mainTable ? mainRow[fieldName] : joinRow[fieldName];
                return acc;
            }, {});
        });
    });
}

function performRightJoin(mainData, joinData, joinCondition, fields, mainTable) {
    return joinData.flatMap(joinRow => {
        const matchingMainRows = mainData.filter(mainRow => mainRow[joinCondition.left.split('.')[1]] === joinRow[joinCondition.right.split('.')[1]]);
        if (matchingMainRows.length === 0) {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === mainTable ? null : joinRow[fieldName];
                return acc;
            }, {});
        }
        return matchingMainRows.map(mainRow => {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === mainTable ? mainRow[fieldName] : joinRow[fieldName];
                return acc;
            }, {});
        });
    });
}

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinType, joinTable, joinCondition } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Logic for applying JOINs
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case 'INNER':
                data = performInnerJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'LEFT':
                data = performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = performRightJoin(data, joinData, joinCondition, fields, table);
                break;
        }
    }

    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
        : data;

    // Select the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

module.exports = executeSELECTQuery;
