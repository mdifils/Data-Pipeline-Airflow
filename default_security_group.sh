aws ec2 describe-security-groups \
--filters Name=group-name,Values=default \
--query SecurityGroups[0].IpPermissions
