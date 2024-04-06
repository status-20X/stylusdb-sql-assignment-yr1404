function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)(=|!=|>=|<=|>|<)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        if (conditionString.includes(' LIKE ')) {
            const [field, pattern] = conditionString.split(/\sLIKE\s/i);
            return { field: field.trim(), operator: 'LIKE', value: pattern.trim().replace(/^'(.*)'$/, '$1') };
        } else {
            const match = conditionString.match(conditionRegex);
            if (match) {
                const [, field, operator, value] = match;
                return { field: field.trim(), operator, value: value.trim() };
            }
            throw new Error('Invalid WHERE clause format');
        }
    });
}

function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null
    };
}

function checkAggregateWithoutGroupBy(query, groupByFields) {
    const aggregateFunctionRegex = /(\bCOUNT\b|\bAVG\b|\bSUM\b|\bMIN\b|\bMAX\b)\s*\(\s*(\*|\w+)\s*\)/i;
    return aggregateFunctionRegex.test(query) && !groupByFields;
}

function parseQuery(query) {

    // Trim the query to remove any leading/trailing whitespaces
    query = query.trim();

    // Process ORDER BY clause and remove it for further processing
    const orderByRegex = /\sORDER BY\s(.+)/i;
    const orderByMatch = query.match(orderByRegex);
    let orderByFields = null;
    if (orderByMatch) {
        orderByFields = orderByMatch[1].split(',').map(field => {
            const [fieldName, order] = field.trim().split(/\s+/);
            return { fieldName, order: order ? order.toUpperCase() : 'ASC' };
        });
        query = query.replace(orderByRegex, '');
    }

    // Process ORDER BY clause and remove it for further processing
    const groupByRegex = /\sGROUP BY\s(.+)/i;
    const groupByMatch = query.match(groupByRegex);
    let groupByFields = null;
    if (groupByMatch) {
        groupByFields = groupByMatch[1].split(',').map(field => field.trim());
        query = query.replace(groupByRegex, '');
    }

    // Process WHERE clause
    const whereSplit = query.split(/\sWHERE\s/i);
    const queryWithoutWhere = whereSplit[0]; // Everything before WHERE clause
    const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;

    const hasAggregateWithoutGroupBy = checkAggregateWithoutGroupBy(query, groupByFields);

    // Process JOIN clause
    const joinSplit = queryWithoutWhere.split(/\s(INNER|LEFT|RIGHT) JOIN\s/i);
    const selectPart = joinSplit[0].trim(); // Everything before JOIN clause

    // Extract JOIN information
    const { joinType, joinTable, joinCondition } = parseJoinClause(queryWithoutWhere);

    // Parse SELECT part
    const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;
    const selectMatch = selectPart.match(selectRegex);
    if (!selectMatch) {
        throw new Error('Invalid SELECT format');
    }
    let [, fields, table] = selectMatch;

    // Parse WHERE part if it exists
    let whereClauses = [];
    if (whereClause) {
        whereClauses = parseWhereClause(whereClause);
    }

    return {
        fields: fields.split(',').map(field => field.trim()),
        table: table.trim(),
        whereClauses,
        joinType,
        joinTable,
        joinCondition,
        groupByFields,
        hasAggregateWithoutGroupBy,
        // orderByFields
    };

}

module.exports = { parseQuery, parseJoinClause };
