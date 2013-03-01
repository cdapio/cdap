#!bin/sh
## Script to clean up older nonce values
## Will be run as a cron 
# $1 -> User
# $2 -> Password
curr_date=`date "+%Y%m%d%H%M%S"`
echo $curr_date
mysql -u $1 -p$2 -e "set @CURR_DATE=$curr_date; use passport; DELETE FROM NONCE where nonce_expires_at < @CURR_DATE"
