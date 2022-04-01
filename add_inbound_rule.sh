defaultSecurityGroup=$(aws ec2 describe-security-groups \
--filters Name=group-name,Values=default \
--query SecurityGroups[0].GroupId \
--output text)

aws ec2 authorize-security-group-ingress \
--group-id $defaultSecurityGroup \
--ip-permissions IpProtocol=tcp,FromPort=0,ToPort=5500,IpRanges='[{CidrIp=0.0.0.0/0}]'
