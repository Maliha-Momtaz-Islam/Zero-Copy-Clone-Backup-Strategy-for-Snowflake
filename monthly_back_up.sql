CREATE OR REPLACE PROCEDURE monthly_backup_purge()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var dbName = '_DEV_COMMON';
var today = new Date().toISOString().slice(0, 10).replace(/-/g, ''); // YYYYMMDD format
var lastMonth = new Date(new Date().setMonth(new Date().getMonth() - 1)).toISOString().slice(0, 10).replace(/-/g, ''); // Date of last month

var monthlyCloneName = 'MONTHLY_CLONE_' + dbName + '_' + today;
var lastMonthCloneName = 'MONTHLY_CLONE_' + dbName + '_' + lastMonth;

try {
    // Retrieve CCR ID from DL_CCR table
    var ccrQuery = `SELECT "id" FROM ${dbName}._DEV_COMMON_ANALYTICS.DL_CCR LIMIT 1`;
    var ccrStmt = snowflake.createStatement({sqlText: ccrQuery});
    var resultSet = ccrStmt.execute();
    var ccrIdValue = resultSet.next() ? resultSet.getColumnValue(1) : null;

    if (ccrIdValue) {
        // Create this month's clone
        var createStmt = snowflake.createStatement({
            sqlText: `CREATE DATABASE ${monthlyCloneName} CLONE ${dbName}`
        });
        createStmt.execute();

        // Log the creation in the backup log table
        var insertLogCreate = snowflake.createStatement({
            sqlText: `
                INSERT INTO ${dbName}._DEV_COMMON_ANALYTICS.CLONE_BACKUP_LOG 
                (ACTION, CLONE_NAME, ACTION_DATE,

USER_ACCOUNT, CCR_ID) VALUES ('CREATE', '${monthlyCloneName}', CURRENT_TIMESTAMP, CURRENT_USER(), '${ccrIdValue}') ` }); insertLogCreate.execute();
   // Drop last month's clone
    var dropStmt = snowflake.createStatement({
        sqlText: `DROP DATABASE IF EXISTS ${lastMonthCloneName}`
    });
    dropStmt.execute();

    // Log the deletion in the backup log table
    var insertLogDelete = snowflake.createStatement({
        sqlText: `
            INSERT INTO ${dbName}._DEV_COMMON_ANALYTICS.CLONE_BACKUP_LOG 
            (ACTION, CLONE_NAME, ACTION_DATE, USER_ACCOUNT, CCR_ID)
            VALUES ('DELETE', '${lastMonthCloneName}', CURRENT_TIMESTAMP, CURRENT_USER(), '${ccrIdValue}')
        `
    });
    insertLogDelete.execute();

    return 'Monthly clone created and last month\'s clone purged successfully.';

} else {
    throw new Error('No CCR ID found in DL_CCR table.');
}

} catch (err) { return 'Error during monthly backup and purge process: ' + err.message; }

