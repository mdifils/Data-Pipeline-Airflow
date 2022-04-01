aws redshift create-cluster \
--cluster-identifier redshiftcluster \
--cluster-type multi-node \
--node-type dc2.large \
--number-of-nodes 4 \
--db-name redshiftdb \
--master-username redshiftuser \
--master-user-password Passw0rd \
--publicly-accessible \
--enhanced-vpc-routing
