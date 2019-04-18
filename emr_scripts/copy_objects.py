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
import argparse


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

    def copy_object(self, bucket, key, copy_acls):
        dest_bucket = self._s3.Bucket(bucket)
        dest_obj = dest_bucket.Object(key)

        src_bucket = self._s3.Bucket(bucket)
        src_obj = src_bucket.Object(key)

        # Get the S3 Object's Storage Class, Metadata, 
        # and Server Side Encryption
        storage_class, metadata, sse_type, last_modified = \
            self._get_object_attributes(src_obj)

        # Update the Metadata so the copy will work
        metadata['forcedreplication'] = runtime

        # Get and copy the current ACL
        if copy_acls:
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

        # Set Server Side Encryption
        if sse_type == 'AES256':
            params['ServerSideEncryption'] = 'AES256'
        elif sse_type == 'aws:kms':
            kms_key = src_obj.ssekms_key_id
            params['ServerSideEncryption'] = 'aws:kms'
            params['SSEKMSKeyId'] = kms_key

        # Copy the S3 Object over the top of itself, 
        # with the Storage Class, updated Metadata, 
        # and Server Side Encryption
        result = dest_obj.copy_from(**params)

        # Put the ACL back on the Object
        if copy_acls:
            dest_obj.Acl().put(AccessControlPolicy=dest_acl)

        return {
            'CopyInPlace': 'TRUE',
            'LastModified': str(result['CopyObjectResult']['LastModified'])
        }


def copy_rows(rows, copy_acls):
    objectutil = ObjectUtil()
    results = []

    for row in rows:
        bucket = str(row['bucket']).strip('"')
        key = str(row['key']).strip('"')

        result = objectutil.copy_object(bucket, key, copy_acls)
        results.append(
            [bucket, key, result['CopyInPlace'], result['LastModified']])

    return results


def copy_objects(spark, inventory_table, inventory_date, partitions, copy_acls):
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

    return crr_failed.rdd.mapPartitions(lambda row: copy_rows(row, copy_acls))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('INVENTORY_TABLE',
        help='Name of the Inventory table in AWS Glue (e.g. "default.crr_preexisting_demo")')
    parser.add_argument('INVENTORY_DATE',
        help='Date of the Inventory to query (e.g. "2019-02-24-04-00"')
    parser.add_argument('OUTPUT_PATH',
        help='S3 Output location (e.g. "s3://crr-preexisting-demo-inventory/results/")')
    parser.add_argument('-p', '--partitions',
        help='Spark repartition optimization',
        type=int,
        default=None)
    parser.add_argument('--acls',
        help='Copies ACLs on S3 objects during the copy-in-place. ' +
            'This involves extra API calls, so should only be used if ACLs are in place',
        action='store_true')

    args = parser.parse_args()

    inventory_table = args.INVENTORY_TABLE
    inventory_date = args.INVENTORY_DATE
    output_path = args.OUTPUT_PATH
    partitions = args.partitions
    acls = args.acls

    spark = SparkSession \
        .builder \
        .appName("CRR Pre-Existing Objects - Copy in Place") \
        .enableHiveSupport() \
        .getOrCreate()

    copied_objects = copy_objects(spark, inventory_table, inventory_date, partitions, acls)
    copied_objects \
        .map(lambda x: ','.join(x)) \
        .saveAsTextFile(output_path)
