#
# CRR Pre-Existing Objects Demo
#
# This Spark job uses CSV files created by S3 inventory to find
# objects that failed during Cross Region Replication. It does "Copy in Place"
# of these objects to trigger another Cross Region Replication attempt.
#
# Positional Inputs:
# - INVENTORY_TABLE: (required) s3:// style path of input inventory files
# - INVENTORY_DATE: (required) S3 inventory date/time string (eg. 2018-09-23-08-00)
# - OUTPUT_PATH: (required) s3:// style output path
# - PARTITIONS: (optional) number of partitions to split input dataframe
#
# Output:
# - CSV_RESULT

from __future__ import print_function
from pyspark.sql import SparkSession

import sys
import boto3
import datetime
import logging


runtime = datetime.datetime.utcnow().isoformat()


class ObjectUtil:

    # static s3 client resource for sharing connections
    _s3 = None

    @classmethod
    def _init_s3(cls):
        if cls._s3 is None:
            print('Initialzing S3 Resource Client')
            cls._s3 = boto3.resource('s3')

    def __init__(self):
        self._init_s3()

    def _get_object_attributes(self, src_obj):
        # Get original Storage Class, Metadata, SSE Type, and ACL
        storage_class = src_obj.storage_class \
            if src_obj.storage_class \
            else 'STANDARD'
        metadata = src_obj.metadata \
            if src_obj.metadata \
            else {}
        sse_type = src_obj.server_side_encryption \
            if src_obj.server_side_encryption \
            else 'None'
        last_modified = src_obj.last_modified

        return storage_class, metadata, sse_type, last_modified

    def copy_object(self, bucket, key):
        dest_bucket = self._s3.Bucket(bucket)
        dest_obj = dest_bucket.Object(key)

        src_bucket = self._s3.Bucket(bucket)
        src_obj = src_bucket.Object(key)

        storage_class, metadata, sse_type, last_modified = \
            self._get_object_attributes(src_obj)

        # Update the Metadata so the copy will work
        metadata['forcedreplication'] = runtime

        # Get and copy the current ACL
        src_acl = src_obj.Acl()
        src_acl.load()
        dest_acl = {
            'Grants': src_acl.grants,
            'Owner': src_acl.owner
        }

        params = {
            'CopySource': {
                'Bucket': bucket,
                'Key': key
            },
            'MetadataDirective': 'REPLACE',
            'TaggingDirective': 'COPY',
            'Metadata': metadata,
            'StorageClass': storage_class
        }

        if sse_type == 'AES256':
            params['ServerSideEncryption'] = 'AES256'
        elif sse_type == 'aws:kms':
            kms_key = src_obj.ssekms_key_id
            params['ServerSideEncryption'] = 'aws:kms'
            params['SSEKMSKeyId'] = kms_key

        result = dest_obj.copy_from(**params)

        # Put the ACL back on the Object
        dest_obj.Acl().put(AccessControlPolicy=dest_acl)

        return {
            'CopyInPlace': 'TRUE',
            'LastModified': str(result['CopyObjectResult']['LastModified'])
        }


def copy_rows(rows):
    objectutil = ObjectUtil()
    results = []

    for row in rows:
        bucket = str(row['bucket']).strip('"')
        key = str(row['key']).strip('"')

        result = objectutil.copy_object(bucket, key)
        results.append(
            [bucket, key, result['CopyInPlace'], result['LastModified']])

    return results


def copy_objects(spark, inventory_table, inventory_date, partitions):
    query = """
        SELECT bucket, key
        FROM {}
        WHERE dt = '{}'
        AND (replication_status = '""'
        OR replication_status = '"FAILED"')
        """.format(inventory_table, inventory_date)
    print('Query: {}'.format(query))

    crr_failed = spark.sql(query)
    # Print the top 20 rows
    crr_failed.show()

    if partitions:
        print('Repartitioning to {}'.format(partitions))
        crr_failed = crr_failed.repartition(partitions)

    return crr_failed.rdd.mapPartitions(copy_rows)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print('Usage: spark-submit {} [INVENTORY_TABLE] [INVENTORY_DATE] [OUTPUT_PATH] [PARITIONS]'.format(sys.argv[0]))
        print('Example:')
        print('')
        print('spark-submit {} default.crr_preexisting_demo 2019-02-24-04-00 s3://crr-preexisting-demo-inventory/results/ 40'.format(sys.argv[0]))
        sys.exit(-1)

    inventory_table = sys.argv[1]
    inventory_date = sys.argv[2]
    output_path = sys.argv[3]

    if len(sys.argv) > 4:
        partitions = int(sys.argv[4])
    else:
        partitions = None

    spark = SparkSession \
        .builder \
        .appName("CRR Pre-Existing Objects - Copy in Place") \
        .enableHiveSupport() \
        .getOrCreate()

    copied_objects = copy_objects(spark, inventory_table, inventory_date, partitions)
    copied_objects \
        .map(lambda x: ','.join(x)) \
        .saveAsTextFile(output_path)
