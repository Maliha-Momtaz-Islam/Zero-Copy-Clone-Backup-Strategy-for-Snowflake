CREATE OR REPLACE PROCEDURE daily_backup_purge()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var dbName = 'DEV_COMMON';
var today = new Date().toISOString().slice(0, 10).replace(/-/g, ''); // YYYYMMDD format
var yesterday = new Date(new Date().setDate(new Date().getDate() - 1)).toISOString().slice(0, 10).replace(/-/g, ''); // Date of 1 day ago

var dailyCloneName = 'DAILY_CLONE_' + dbName + '_' + today;
var yesterdayCloneName = 'DAILY_CLONE_' + dbName + '_' + yesterday;

try {
    // Retrieve CCR ID from DL_CCR table
    var ccrQuery = `SELECT "id" FROM ${dbName}._DEV_COMMON_ANALYTICS.DL_CCR LIMIT 1`;
    var ccrStmt = snowflake.createStatement({sqlText: ccrQuery});
    var resultSet = ccrStmt.execute();
    var ccrIdValue = resultSet.next() ? resultSet.getColumnValue(1) : null;

    if (ccrIdValue) {
        // Create today's clone
        var createStmt = snowflake.createStatement({
            sqlText: `CREATE DATABASE ${dailyCloneName} CLONE ${dbName}`
        });
        createStmt.execute();

        // Log the creation in the backup log table
        var insertLogCreate = snowflake.createStatement({
            sqlText: `
                INSERT INTO ${dbName}._DEV_COMMON_ANALYTICS.CLONE_BACKUP_LOG 
                (ACTION, CLONE_NAME, ACTION_DATE, USER_ACCOUNT, CCR_ID)
                VALUES ('CREATE', '${dailyCloneName}', CURRENT_TIMESTAMP, CURRENT_USER(), '${ccrIdValue}')
            `
        });
        insertLogCreate.execute();

        // Drop yesterday's clone
        var dropStmt = snowflake.createStatement({
            sqlText: `DROP DATABASE IF EXISTS ${yesterdayCloneName}`
        });
        dropStmt.execute();

        // Log the deletion in the backup log table
        var insertLogDelete = snowflake.createStatement({
            sqlText: `
                INSERT INTO ${dbName}.DEV_COMMON_ANALYTICS.CLONE_BACKUP_LOG 
                (ACTION, CLONE_NAME, ACTION_DATE, USER_ACCOUNT, CCR_ID)
                VALUES ('DELETE', '${yesterdayCloneName}', CURRENT_TIMESTAMP, CURRENT_USER(), '${ccrIdValue}')
            `
        });
        insertLogDelete.execute();

        return 'Daily clone created and previous day clone purged successfully.';

    } else {
        throw new Error('No CCR ID found in DL_CCR table.');
    }

} catch (err) {
    return 'Error during daily backup and purge process: ' + err.message;
}
$$;

