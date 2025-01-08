# Zero-Copy-Clone-Backup-Strategy-for-Snowflake
This approach ensures daily, weekly, and monthly clones are created and managed automatically in Snowflake. The stored procedures are designed to log each action and manage retention periods effectively. We'll also cover how to schedule tasks and test the procedures to confirm everything works as expected.

Project Requirements
Daily Clones: Retained for 1 day and purged the next day.
Weekly Clones: Retained for 1 week, created at the end of each week.
Monthly Clones: Retained for 1 month, created at the start of each month.
Logging: Each clone creation and deletion action should be logged, including the CCR ID from the DL_CCR table, user account, and timestamp.
Step-by-Step Implementation

1. Create a Log Table
To track clone creation and deletion actions, set up a log table in your database:
CREATE OR REPLACE TABLE clone_backup_log (
  action STRING,            -- 'CREATE' or 'DELETE'
  clone_name STRING,        -- Name of the clone
  action_date TIMESTAMP,    -- Date and time of action
  user_account STRING,      -- User account performing the action
  ccr_id STRING             -- CCR ID for tracking
);

2. Stored Procedure for Daily Backup and Purge
The following procedure creates a daily clone and deletes the previous day's clone while logging the action details:
Daily Backup Procedure Script/ weekly/monthly


3. Schedule Tasks for Automation 
### Daily Backup Task ```sql CREATE OR REPLACE TASK daily_backup_task SCHEDULE = 'USING CRON 0 1 * * *' -- Runs daily at 1 AM COMMENT = 'Daily backup task for cloning' AS CALL daily_backup_purge(); ``` 

### Weekly Backup Task ```sql CREATE OR REPLACE TASK weekly_backup_task SCHEDULE = 'USING CRON 0 2 * * SUN' -- Runs weekly on Sunday at 2 AM COMMENT = 'Weekly backup task for cloning' AS CALL weekly_backup_purge(); ``` 

### Monthly Backup Task ```sql CREATE OR REPLACE TASK monthly_backup_task SCHEDULE = 'USING CRON 0 3 1 * *' -- Runs on the 1st of every month at 3 AM COMMENT = 'Monthly backup task for cloning' AS CALL monthly_backup_purge(); ``` ## Testing the Implementation ### Manual Execution Run the stored procedures manually to verify they create and log clones as expected: ```sql CALL daily_backup_purge(); CALL weekly_backup_purge(); CALL monthly_backup_purge(); ``` 

### Verify Clones Check if the clones are present using: ```sql SHOW DATABASES LIKE 'DAILY_CLONE_%'; SHOW DATABASES LIKE 'WEEKLY_CLONE_%'; SHOW DATABASES LIKE 'MONTHLY_CLONE_%'; ``` ### Review Logs Ensure the `CLONE_BACKUP_LOG` table is updated with the correct `action`, `clone_name`, `action_date`, `user_account`.
