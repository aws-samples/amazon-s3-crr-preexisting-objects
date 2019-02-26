#!/bin/bash

# EMR cluster variables
AWS_PROFILE=  # 'default'
REGION= # 'us-east-1'
SUBNET_ID= # 'subnet-12345678'
EMR_CLUSTER_NAME='CrrPreexistingDemo'
INVENTORY_BUCKET='crr-preexisting-demo-inventory'
MASTER_INSTANCE_TYPE='m4.xlarge'
CORE_INSTANCE_TYPE='m4.2xlarge'
CORE_INSTANCE_COUNT='1'
EMR_RELEASE='emr-5.17.0'

# EMR job variables
GLUE_DATABASE_NAME='default'
ATHENA_TABLE_NAME='crr_preexisting_demo'
INVENTORY_DATE='2019-02-24-04-00'
PARTITIONS='1'

# Create default EMR roles
aws --profile $AWS_PROFILE emr create-default-roles

# Copy files
aws --profile $AWS_PROFILE s3 cp emr_scripts/bootstrap.sh s3://${INVENTORY_BUCKET}/emr/bootstrap.sh
aws --profile $AWS_PROFILE s3 cp emr_scripts/step_0.sh s3://${INVENTORY_BUCKET}/emr/step_0.sh
aws --profile $AWS_PROFILE s3 cp emr_scripts/copy_objects.py s3://${INVENTORY_BUCKET}/emr/copy_objects.py

# Clear the Spark application's results table
aws --profile $AWS_PROFILE s3 rm s3://${INVENTORY_BUCKET}/results --recursive

# Launch EMR cluster
aws emr create-cluster \
  --profile $AWS_PROFILE \
  --applications Name=Spark Name=Hadoop \
  --name $EMR_CLUSTER_NAME \
  --region $REGION \
  --ebs-root-volume-size 10 \
  --release-label $EMR_RELEASE \
  --service-role EMR_DefaultRole \
  --auto-scaling-role EMR_AutoScaling_DefaultRole \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --tags 'Name='$EMR_CLUSTER_NAME'' \
  --instance-groups '[{"InstanceCount":'$CORE_INSTANCE_COUNT',"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"'$CORE_INSTANCE_TYPE'","Name":"Core"},
                      {"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"'$MASTER_INSTANCE_TYPE'","Name":"Master"}]' \
  --log-uri 's3n://'$INVENTORY_BUCKET'/emr/logs/' \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=$SUBNET_ID \
  --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"},"Configurations":[]}]' \
  --bootstrap-actions '[{"Path":"s3://'$INVENTORY_BUCKET'/emr/bootstrap.sh","Name":"Custom action"}]' \
  --steps '[{"Args":["s3://'$INVENTORY_BUCKET'/emr/step_0.sh","s3://'$INVENTORY_BUCKET'/emr/copy_objects.py"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://'$REGION'.elasticmapreduce/libs/script-runner/script-runner.jar","Properties":"","Name":"Download Spark Script"},
            {"Args":["spark-submit","--deploy-mode","client","/home/hadoop/copy_objects.py","'$GLUE_DATABASE_NAME'.'$ATHENA_TABLE_NAME'","'$INVENTORY_DATE'","s3://'$INVENTORY_BUCKET'/results/","'$PARTITIONS'"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Run Spark Application"}]'
