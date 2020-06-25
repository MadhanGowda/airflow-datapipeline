"""Utils for the DAG functions."""
import base64
import boto3
import json
import os
import psycopg2
from psycopg2.extras import execute_values

from datetime import datetime

from airflow.utils.email import send_email
from botocore.exceptions import ClientError


def get_environ(env, default=None):
    """Get value from ENV."""
    val = os.getenv(env, default)
    return val


def get_failure_email_list():
    """Failure email list."""
    email_list = get_environ(
        "DATAPIPELINE_EMAILS",
        ['devops@galepartners.com']
    )
    return email_list


def get_secret(secret_name):
    """Genrate Credntials for the db."""
    region_name = get_environ("AWS_REGION", 'us-west-2')

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    print(secret_name)

    # In this sample we only handle the specific exceptions for the
    # 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/
    # API_GetSecretValue.html
    # We rethrow the exception by default.
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using
            # the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current
            # state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary,
        # one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(
                get_secret_value_response['SecretBinary']
            )
        return secret


class DatabaseConnection(object):
    """Connect to database class."""

    def initalize(self, params):
        """Initalize the db connections."""
        self.query = params['query']
        self.database = params['database']
        if self.database == 'REDSHIFT':
            secret_name = get_environ('REDSHIFT_CREDENTIALS_SECRET_NAME')
        elif self.database == 'POSTGRESS':
            secret_name = get_environ('RDS_CREDENTIALS_SECRET_NAME')
        elif self.database == 'POSTGRES_ALCHEMY_APP':
            secret_name = get_environ('POSTGRES_ALCHEMY_DB_SECRET_NAME')
        else:
            raise Exception('Supported only REDSHIFT and POSTGRESS.')
        return secret_name

    def run_query(self, params):
        """Main function."""
        secret_name = self.initalize(params)

        self.get_db_config(secret_name)
        self.create_connection()

        try:
            data = []
            data = self.execute_query()
            print('done')
        except Exception as e:
            print(e)
            raise Exception(e)
        finally:
            print('finally')
            self.close_connection()
        return data

    def transction_query(self, params):
        """Main function."""
        secret_name = self.initalize(params)

        self.get_db_config(secret_name)
        self.create_connection()

        try:
            data = []
            data = self.execute_transction_query()
        except Exception as e:
            raise Exception(e)
        finally:
            self.connection.close()
        return data

    def create_connection(self):
        """Create db Conection."""
        print('create connection...')
        config = self.config
        conn = psycopg2.connect(
            dbname=config['dbname'],
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['pwd']
        )
        print('completed connection...')
        self.connection = conn

    def execute_query(self):
        """Execute query on DB."""
        cursor = self.connection.cursor()
        print('Executing Query')
        cursor.execute(self.query)
        print('execution finished, fetching results...')
        data = []
        if cursor.description:
            print(cursor.description)
            data = cursor.fetchall()
        print('query executing finished. Data:')
        print(data)
        cursor.close()
        return data

    def execute_transction_query(self):
        """Execute query on DB."""
        cursor = self.connection.cursor()
        op_data = []
        for each_query in self.query:
            if each_query:
                temp_obj = {}
                query = each_query + ';'
                temp_obj['query'] = query
                print('Executing Query >> {}'.format(query))
                cursor.execute(query)
                if cursor.description:
                    print(cursor.description)
                    data = cursor.fetchall()
                    temp_obj['data'] = data
                else:
                    temp_obj['data'] = None
                op_data.append(temp_obj)
                print('Executing finished')
        print(op_data)
        cursor.close()
        self.connection.commit()
        return op_data

    def bulk_load_data_query(self, params, data):
        """Bulk load of data into the destination tables."""
        secret_name = self.initalize(params)

        self.get_db_config(secret_name)
        self.create_connection()

        try:
            cursor = self.connection.cursor()
            try:
                print('Executing Bulk insert Query on RDS')
                # bulk load data into the table.
                # https://hakibenita.com/fast-load-data-python-postgresql,
                # performance comparison.
                print(self.query)
                execute_values(cursor, self.query, data)

            except Exception as e:
                print('ERROR: Cannot execute cursor ', e)
                raise Exception(e)

            try:
                results_list = []
                for result in cursor:
                    results_list.append(result)
                print(results_list)
            except psycopg2.ProgrammingError:
                pass

            cursor.close()
            self.connection.commit()
            return results_list

        except Exception as e:
            print('ERROR: Cannot connect to database from handler ', e)
            raise Exception(e)

    def close_connection(self):
        """Close connection."""
        print('commmiting...')
        self.connection.commit()
        print('closing connection')
        self.connection.close()

    def get_db_config(self, secret_name):
        """Get the db configurations."""
        print("get_{}_configuration...".format(self.database))
        credentials = json.loads(get_secret(secret_name))
        configuration = {
            'dbname': credentials.get('database'),
            'user': credentials.get('dbuser'),
            'pwd': credentials.get('password'),
            'host': credentials.get('endpoint'),
            'port': credentials.get('port')
        }
        self.config = configuration


def get_bucket_and_prefix(path):
    """Get Bucket and Prefix."""
    path_array = path.split('/')
    bucket = path_array[2]
    addtional_path = '/'.join(path_array[3:])
    return bucket, addtional_path


class S3Connection(object):
    """Connect to S3 class."""

    def __init__(self):
        """Initializing function."""
        self.client = boto3.client('s3')

    def get_list_of_files(self, params):
        """Get paginated list of all the files in the path."""
        print('Getting file list from S3 for bucket {} and path {}'.format(
            params['bucket'],
            params['prefix']
        ))
        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=params['bucket'],
            Prefix=params['prefix']
        )
        file_list = []
        for page in pages:
            file_list += page.get('Contents', [])
        print('Found all file list from S3')
        return file_list

    def get_file_info(self, params):
        """Get files info from the path."""
        response = self.client.list_objects_v2(
            Bucket=params['bucket'],
            Prefix=params['prefix'],
        )
        file_meta_data = response.get('Contents', [])
        return file_meta_data

    def delete_folder(self, params):
        """Delete a folder."""
        file_list = self.get_list_of_files(params)
        print('Found file list', file_list)
        for obj in file_list:
            file_path = obj['Key']
            self.client.delete_object(
                Bucket=params['bucket'],
                Key=file_path
            )


def format_data_from_db(header, data):
    """Format the data from DB."""
    print("Formating the data...")
    output_data = []
    for each in data:
        temp_obj = {}
        for indx, column_name in enumerate(header):
            temp_obj[column_name] = each[indx]
        output_data.append(temp_obj)

    return output_data


def notify_success_email(email_data):
    """Send custom email alerts."""
    title = "CCLS Airflow Success Alert: {0}".format(
        email_data['dag_id']
    )

    date = str(datetime.now())

    body = """
    Hi,<br>
    <br>
    <b>{0}</b> job ran <b>successfully</b> at {1}.<br>
    <br>
    Thanks and Regards,<br>
    <b>CCLS Airflow Bot</b><br>
    """.format(
        email_data['dag_id'],
        date
    )
    print('Sending success email')
    send_email(
        email_data['email'],
        title,
        body
    )
