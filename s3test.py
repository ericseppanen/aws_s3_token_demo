#!/usr/bin/python3

# This program demonstrates use of AWS STS "federated" tokens for limited
# access to a single "folder" in AWS S3. Note "folder" is really just a name
# prefix.
#
#
# To try this yourself:
# Create a bucket under your aws account. Create some sub-folders and
# place some files inside the folders.
#
# Demonstrate that you can read the folder and file using your own
# user credentials (e.g. using env variables AWS_ACCESS_KEY_ID and
# AWS_SECRET_ACCESS_KEY):
#    ./s3test.py --env --bucket MYBUCKET --folder TEST1234 read file1234.txt
#
# Create a federated auth token with limited capabilities:
#    ./s3test.py --bucket MYBUCKET --folder timeline1234 auth
#
# List the folder:
#    ./s3test.py --bucket MYBUCKET --folder timeline1234 list
#
# Read a file:
#    ./s3test.py --bucket MYBUCKET --folder timeline1234 read test1234.txt
#
# These operations will fail (Access Denied), because the token does not permit
# access to this "folder".
#    ./s3test.py --bucket MYBUCKET --folder timeline1235 list
#    ./s3test.py --bucket MYBUCKET --folder timeline1235 read test1235.txt
#
# Note: It's possible to verify that the folder and objects do exist, using the
# --env flag to use the user credentials instead of the federated token.
#
# If you wait 15 minutes, the token will expire and all commands will fail (with
# an ExpiredToken error), until you re-run the 'auth' command.


import argparse
import boto3
import json
from pprint import pprint
import sys

DEFAULT_CREDS_FILE = 'creds.json'


def get_creds(bucket_name, folder_name):
    """ Use AWS STS to generate a token restricted to one "folder".

    The token can only be used to list and read objects inside the
    specified bucket and "folder" (which is really just a name prefix).

    The token will only be valid for 15 minutes.
    """
    client = boto3.client('sts')
    # pprint(client.get_caller_identity())

    # A name of our choice (no spaces allowed)
    credential_name = f'AccessTimeline-{folder_name}'
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                # A name of our choice [0-9a-zA-Z]
                "Sid": f'ListBucket{folder_name}',
                "Action": [
                    "s3:ListBucket",
                ],
                "Effect": "Allow",
                "Resource": [
                    f'arn:aws:s3:::{bucket_name}',
                ],
                "Condition": {
                    "StringEquals": {
                        "s3:prefix": [
                            f'{folder_name}/'
                        ],
                        "s3:delimiter": [
                            '/'
                        ],
                    }
                }

            },
            {
                "Sid": f'GetObject{folder_name}',
                "Action": [
                    "s3:GetObject",
                ],
                "Effect": "Allow",
                "Resource": [
                    f'arn:aws:s3:::{bucket_name}/{folder_name}/*',
                ],

                # FIXME: it should be possible to place restrictions using Condition,
                # but I haven't been able to get this to work.
                #
                # "Condition": {
                #     "StringLike": {
                #         "s3:prefix": [
                #             f'{folder_name}/*'
                #         ],
                #         "s3:delimiter": [
                #             '/'
                #         ],
                #     }
                # }
            },
        ]
    }

    # pprint(policy)
    policy = json.dumps(policy)
    credential_lifetime = 900  # 900 seconds is the minimum value

    creds = client.get_federation_token(
        Name=credential_name,
        Policy=policy,
        DurationSeconds=credential_lifetime,
    )

    # pprint(creds)
    return creds['Credentials']


def s3_client_creds(creds):
    ''' Create an s3 client object using specified credentials. '''
    fed_key_id = creds['AccessKeyId']
    fed_secret = creds['SecretAccessKey']
    fed_token = creds['SessionToken']

    print('Now use those credentials and try to access s3 resources...')

    fed_client = boto3.client(
        's3',
        aws_access_key_id=fed_key_id,
        aws_secret_access_key=fed_secret,
        aws_session_token=fed_token
    )
    return fed_client


def s3_client_plain():
    ''' Create an s3 client object using credentials from the environment. '''
    return boto3.client('s3')


def list_objects(s3_client, bucket_name, folder_name):
    response = s3_client.list_objects(
        Bucket=bucket_name,
        Delimiter='/',
        Prefix=f'{folder_name}/',
    )

    # pprint(response)
    print('Folder contents:')
    for obj in response['Contents']:
        print('  ' + obj['Key'])


def get_object(s3_client, bucket_name, folder_name, file_name):
    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=f'{folder_name}/{file_name}',
    )

    body = response['Body'].read().decode('utf-8')
    print('Contents of file:')
    print(body)


def save_creds(creds):
    with open('creds.json', 'w') as f:
        # hack: datetime doesn't serialize to json; we don't need it anyway.
        del creds['Expiration']
        json.dump(creds, f)
    print('Saved credentials.')


def load_creds():
    with open('creds.json') as f:
        creds = json.load(f)
    print('Loaded credentials.')
    return creds


def main():
    parser = argparse.ArgumentParser()
    # With --env, the aws auth strings will be automatically pulled
    # from the environment using the default boto3 behavior.
    # Without --env, we will load `creds.json`
    parser.add_argument(
        '--env', help='AWS auth from environment', action='store_true')
    parser.add_argument('--creds', help='auth token filename',
                        default=DEFAULT_CREDS_FILE)
    parser.add_argument('--bucket', help='bucket name', required=True)
    parser.add_argument('--folder', help='"folder" prefix', required=True)
    subparsers = parser.add_subparsers(dest='subcmd')
    parser_auth = subparsers.add_parser('auth', help='create auth token (implies --env)')
    parser_read = subparsers.add_parser('read', help='read an object')
    parser_read.add_argument('object_name', metavar='OBJECT-NAME')
    parser_list = subparsers.add_parser('list', help='list objects')

    args = parser.parse_args()

    # Retrieve a federated token from AWS, with
    # limited access permissions. Store the credentials to
    # a JSON file for later use.
    if args.subcmd == 'auth':
        creds = get_creds(args.bucket, args.folder)
        save_creds(creds)
        sys.exit(0)

    if args.env:
        # Use plain user credentials (not using the federated token).
        client = s3_client_plain()
    else:
        # Load credentials (saved using the 'auth' subcommand).
        creds = load_creds()
        client = s3_client_creds(creds)

    # List objects in the "folder".
    if args.subcmd == 'list':
        list_objects(client, args.bucket, args.folder)

    # Read an object.
    if args.subcmd == 'read':
        get_object(client, args.bucket, args.folder, args.object_name)


main()
