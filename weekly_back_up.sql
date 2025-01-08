CREATE OR REPLACE PROCEDURE weekly_backup_purge()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var dbName = '_DEV_COMMON';
var today = new Date().toISOString().slice(0, 10).replace(/-/g, ''); // YYYYMMDD format
var lastWeek = new Date(new Date().setDate(new Date().getDate() - 7)).toISOString().slice(0, 10).replace(/-/g, ''); // Date of 7 days ago

var weeklyCloneName = 'WEEKLY_CLONE_' + dbName + '_' + today;
var lastWeekCloneName = 'WEEKLY_CLONE_' + dbName + '_' + lastWeek;

try {
    // Retrieve CCR ID from DL_CCR table
    var ccrQuery = `SELECT "id" FROM ${dbName}._DEV_COMMON_ANALYTICS.DL_CCR LIMIT 1`;
    var ccrStmt = snowflake.createStatement({sqlText: ccrQuery});
    var resultSet = ccrStmt.execute();
    var ccrIdValue = resultSet.next() ? resultSet.getColumnValue(1) : null;

    if (ccrIdValue) {
        // Create this week's clone
        var createStmt = snowflake.createStatement({
            sqlText: `CREATE DATABASE ${weeklyCloneName} CLONE ${dbName}`
        });
        createStmt.execute();

        // Log the creation in the backup log table
        var insertLogCreate = snowflake.createStatement({
            sqlText: `
                INSERT INTO ${dbName}._DEV_COMMON_ANALYTICS.CLONE_BACKUP_LOG 
                (ACTION, CLONE_NAME, ACTION_DATE, USER_ACCOUNT, CCR_ID)
                VALUES ('CREATE', '${weeklyCloneName}', CURRENT_TIMESTAMP, CURRENT_USER(), '${ccrIdValue}')
            `
        });
        insertLogCreate.execute();

        // Drop last week's clone
        var dropStmt = snowflake.createStatement({
            sqlText: `DROP DATABASE IF EXISTS ${lastWeekCloneName}`
        });
        dropStmt.execute();

        // Log the deletion in the backup log table
        var insertLogDelete = snowflake.createStatement({
            sqlText: `
                INSERT INTO ${dbName}._DEV_COMMON_ANALYTICS.CLONE_BACKUP_LOG 
                (ACTION, CLONE_NAME, ACTION_DATE, USER_ACCOUNT, CCR_ID)
                VALUES ('DELETE', '${lastWeekCloneName}', CURRENT_TIMESTAMP, CURRENT_USER(), '${ccrIdValue}')
            `
        });
        insertLogDelete.execute();

        return 'Weekly clone created and last week\'s clone purged successfully.';

    } else {
        throw new Error('No CCR ID found in DL_CCR table.');
    }

} catch (err) {
    return 'Error during weekly backup and purge process: ' + err.message;
}
$$;

