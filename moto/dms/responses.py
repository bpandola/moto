import json

from moto.core.responses import BaseResponse

from .models import DatabaseMigrationServiceBackend, dms_backends


class DatabaseMigrationServiceResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="dms")
        self.automated_parameter_parsing = True

    @property
    def dms_backend(self) -> DatabaseMigrationServiceBackend:
        return dms_backends[self.current_account][self.region]

    def create_replication_task(self) -> str:
        replication_task_identifier = self._get_param("ReplicationTaskIdentifier")
        source_endpoint_arn = self._get_param("SourceEndpointArn")
        target_endpoint_arn = self._get_param("TargetEndpointArn")
        replication_instance_arn = self._get_param("ReplicationInstanceArn")
        migration_type = self._get_param("MigrationType")
        table_mappings = self._get_param("TableMappings")
        replication_task_settings = self._get_param("ReplicationTaskSettings")
        tags = self._get_param("Tags")
        replication_task = self.dms_backend.create_replication_task(
            replication_task_identifier=replication_task_identifier,
            source_endpoint_arn=source_endpoint_arn,
            target_endpoint_arn=target_endpoint_arn,
            replication_instance_arn=replication_instance_arn,
            migration_type=migration_type,
            table_mappings=table_mappings,
            replication_task_settings=replication_task_settings,
            tags=tags,
        )

        return json.dumps({"ReplicationTask": replication_task.to_dict()})

    def start_replication_task(self) -> str:
        replication_task_arn = self._get_param("ReplicationTaskArn")
        replication_task = self.dms_backend.start_replication_task(
            replication_task_arn=replication_task_arn
        )

        return json.dumps({"ReplicationTask": replication_task.to_dict()})

    def stop_replication_task(self) -> str:
        replication_task_arn = self._get_param("ReplicationTaskArn")
        replication_task = self.dms_backend.stop_replication_task(
            replication_task_arn=replication_task_arn
        )

        return json.dumps({"ReplicationTask": replication_task.to_dict()})

    def delete_replication_task(self) -> str:
        replication_task_arn = self._get_param("ReplicationTaskArn")
        replication_task = self.dms_backend.delete_replication_task(
            replication_task_arn=replication_task_arn
        )

        return json.dumps({"ReplicationTask": replication_task.to_dict()})

    def describe_replication_tasks(self) -> str:
        filters = self._get_param("Filters", [])
        max_records = self._get_int_param("MaxRecords", 100)
        replication_tasks = self.dms_backend.describe_replication_tasks(
            filters=filters, max_records=max_records
        )

        return json.dumps(
            {"ReplicationTasks": [t.to_dict() for t in replication_tasks]}
        )

    def create_replication_instance(self) -> str:
        replication_instance_identifier = self._get_param(
            "ReplicationInstanceIdentifier"
        )
        allocated_storage = self._get_param("AllocatedStorage")
        replication_instance_class = self._get_param("ReplicationInstanceClass")
        vpc_security_group_ids = self._get_param("VpcSecurityGroupIds")
        availability_zone = self._get_param("AvailabilityZone")
        replication_subnet_group_identifier = self._get_param(
            "ReplicationSubnetGroupIdentifier"
        )
        preferred_maintenance_window = self._get_param("PreferredMaintenanceWindow")
        multi_az = self._get_param("MultiAZ")
        engine_version = self._get_param("EngineVersion")
        auto_minor_version_upgrade = self._get_param("AutoMinorVersionUpgrade")
        tags = self._get_param("Tags")
        kms_key_id = self._get_param("KmsKeyId")
        publicly_accessible = self._get_param("PubliclyAccessible")
        dns_name_servers = self._get_param("DnsNameServers")
        resource_identifier = self._get_param("ResourceIdentifier")
        network_type = self._get_param("NetworkType")
        kerberos_authentication_settings = self._get_param(
            "KerberosAuthenticationSettings"
        )
        replication_instance = self.dms_backend.create_replication_instance(
            replication_instance_identifier=replication_instance_identifier,
            allocated_storage=allocated_storage,
            replication_instance_class=replication_instance_class,
            vpc_security_group_ids=vpc_security_group_ids,
            availability_zone=availability_zone,
            replication_subnet_group_identifier=replication_subnet_group_identifier,
            preferred_maintenance_window=preferred_maintenance_window,
            multi_az=multi_az,
            engine_version=engine_version,
            auto_minor_version_upgrade=auto_minor_version_upgrade,
            tags=tags,
            kms_key_id=kms_key_id,
            publicly_accessible=publicly_accessible,
            dns_name_servers=dns_name_servers,
            resource_identifier=resource_identifier,
            network_type=network_type,
            kerberos_authentication_settings=kerberos_authentication_settings,
        )
        return json.dumps({"ReplicationInstance": replication_instance.to_dict()})

    def describe_replication_instances(self) -> str:
        filters = self._get_param("Filters", [])
        max_records = self._get_param("MaxRecords")
        marker = self._get_param("Marker")

        replication_instances = self.dms_backend.describe_replication_instances(
            filters=filters,
            max_records=max_records,
            marker=marker,
        )

        instances_dict = [i.to_dict() for i in replication_instances]

        # TODO: Add Marker (optional) to the response
        return json.dumps({"ReplicationInstances": instances_dict})

    def delete_replication_instance(self) -> str:
        replication_instance_arn = self._get_param("ReplicationInstanceArn")
        replication_instance = self.dms_backend.delete_replication_instance(
            replication_instance_arn=replication_instance_arn,
        )
        return json.dumps({"ReplicationInstance": replication_instance.to_dict()})

    def create_endpoint(self) -> str:
        endpoint_identifier = self._get_param("EndpointIdentifier")
        endpoint_type = self._get_param("EndpointType")
        engine_name = self._get_param("EngineName")
        username = self._get_param("Username")
        password = self._get_param("Password")
        server_name = self._get_param("ServerName")
        port = self._get_param("Port")
        database_name = self._get_param("DatabaseName")
        extra_connection_attributes = self._get_param("ExtraConnectionAttributes")
        kms_key_id = self._get_param("KmsKeyId")
        tags = self._get_param("Tags")
        certificate_arn = self._get_param("CertificateArn")
        ssl_mode = self._get_param("SslMode")
        service_access_role_arn = self._get_param("ServiceAccessRoleArn")
        external_table_definition = self._get_param("ExternalTableDefinition")
        dynamo_db_settings = self._get_param("DynamoDbSettings")
        s3_settings = self._get_param("S3Settings")
        dms_transfer_settings = self._get_param("DmsTransferSettings")
        mongo_db_settings = self._get_param("MongoDbSettings")
        kinesis_settings = self._get_param("KinesisSettings")
        kafka_settings = self._get_param("KafkaSettings")
        elasticsearch_settings = self._get_param("ElasticsearchSettings")
        neptune_settings = self._get_param("NeptuneSettings")
        redshift_settings = self._get_param("RedshiftSettings")
        postgre_sql_settings = self._get_param("PostgreSQLSettings")
        my_sql_settings = self._get_param("MySQLSettings")
        oracle_settings = self._get_param("OracleSettings")
        sybase_settings = self._get_param("SybaseSettings")
        microsoft_sql_server_settings = self._get_param("MicrosoftSQLServerSettings")
        ibm_db2_settings = self._get_param("IBMDb2Settings")
        resource_identifier = self._get_param("ResourceIdentifier")
        doc_db_settings = self._get_param("DocDbSettings")
        redis_settings = self._get_param("RedisSettings")
        gcp_my_sql_settings = self._get_param("GcpMySQLSettings")
        timestream_settings = self._get_param("TimestreamSettings")
        endpoint = self.dms_backend.create_endpoint(
            endpoint_identifier=endpoint_identifier,
            endpoint_type=endpoint_type,
            engine_name=engine_name,
            username=username,
            password=password,
            server_name=server_name,
            port=port,
            database_name=database_name,
            extra_connection_attributes=extra_connection_attributes,
            kms_key_id=kms_key_id,
            tags=tags,
            certificate_arn=certificate_arn,
            ssl_mode=ssl_mode,
            service_access_role_arn=service_access_role_arn,
            external_table_definition=external_table_definition,
            dynamo_db_settings=dynamo_db_settings,
            s3_settings=s3_settings,
            dms_transfer_settings=dms_transfer_settings,
            mongo_db_settings=mongo_db_settings,
            kinesis_settings=kinesis_settings,
            kafka_settings=kafka_settings,
            elasticsearch_settings=elasticsearch_settings,
            neptune_settings=neptune_settings,
            redshift_settings=redshift_settings,
            postgre_sql_settings=postgre_sql_settings,
            my_sql_settings=my_sql_settings,
            oracle_settings=oracle_settings,
            sybase_settings=sybase_settings,
            microsoft_sql_server_settings=microsoft_sql_server_settings,
            ibm_db2_settings=ibm_db2_settings,
            resource_identifier=resource_identifier,
            doc_db_settings=doc_db_settings,
            redis_settings=redis_settings,
            gcp_my_sql_settings=gcp_my_sql_settings,
            timestream_settings=timestream_settings,
        )

        return json.dumps(
            {"Endpoint": {k: v for k, v in endpoint.to_dict().items() if v is not None}}
        )

    def describe_endpoints(self) -> str:
        filters = self._get_param("Filters", [])
        max_records = self._get_param("MaxRecords")
        marker = self._get_param("Marker")
        endpoints = self.dms_backend.describe_endpoints(
            filters=filters, max_records=max_records, marker=marker
        )
        return json.dumps(
            {
                "Endpoints": [
                    {k: v for k, v in endpoint.to_dict().items() if v is not None}
                    for endpoint in endpoints
                ]
            }
        )

    def delete_endpoint(self) -> str:
        endpoint_arn = self._get_param("EndpointArn")
        endpoint = self.dms_backend.delete_endpoint(
            endpoint_arn=endpoint_arn,
        )
        return json.dumps({"Endpoint": endpoint.to_dict()})

    def list_tags_for_resource(self) -> str:
        resource_arn = self._get_param("ResourceArn")
        resource_arn_list = self._get_param("ResourceArnList")

        if resource_arn and resource_arn_list:
            raise ValueError(
                "Both ResourceArn and ResourceArnList cannot be specified."
            )
        if not resource_arn and not resource_arn_list:
            raise ValueError(
                "Either ResourceArn or ResourceArnList should be specified."
            )

        if resource_arn:
            tag_list = self.dms_backend.list_tags_for_resource([resource_arn])
        else:
            tag_list = self.dms_backend.list_tags_for_resource(resource_arn_list)

        return json.dumps({"TagList": tag_list})

    def create_replication_subnet_group(self) -> str:
        replication_subnet_group_identifier = self._get_param(
            "ReplicationSubnetGroupIdentifier"
        )
        replication_subnet_group_description = self._get_param(
            "ReplicationSubnetGroupDescription"
        )
        subnet_ids = self._get_param("SubnetIds")
        tags = self._get_param("Tags")
        replication_subnet_group = self.dms_backend.create_replication_subnet_group(
            replication_subnet_group_identifier=replication_subnet_group_identifier,
            replication_subnet_group_description=replication_subnet_group_description,
            subnet_ids=subnet_ids,
            tags=tags,
        )
        return json.dumps(
            {"ReplicationSubnetGroup": replication_subnet_group.to_dict()}
        )

    def describe_replication_subnet_groups(self) -> str:
        filters = self._get_param("Filters", [])
        max_records = self._get_param("MaxRecords")
        marker = self._get_param("Marker")
        replication_subnet_groups = self.dms_backend.describe_replication_subnet_groups(
            filters=filters, max_records=max_records, marker=marker
        )

        return json.dumps(
            {
                "ReplicationSubnetGroups": [
                    {
                        k: v
                        for k, v in replication_subnet_group.to_dict().items()
                        if v is not None
                    }
                    for replication_subnet_group in replication_subnet_groups
                ]
            }
        )

    def delete_replication_subnet_group(self) -> str:
        replication_subnet_group_identifier = self._get_param(
            "ReplicationSubnetGroupIdentifier"
        )
        self.dms_backend.delete_replication_subnet_group(
            replication_subnet_group_identifier=replication_subnet_group_identifier,
        )
        return json.dumps({})

    def test_connection(self) -> str:
        replication_instance_arn = self._get_param("ReplicationInstanceArn")
        endpoint_arn = self._get_param("EndpointArn")
        connection = self.dms_backend.test_connection(
            replication_instance_arn=replication_instance_arn,
            endpoint_arn=endpoint_arn,
        )
        return json.dumps({"Connection": connection.to_dict()})

    def describe_connections(self) -> str:
        filters = self._get_param("Filters", [])
        max_records = self._get_param("MaxRecords")
        marker = self._get_param("Marker")

        connections = self.dms_backend.describe_connections(
            filters=filters, max_records=max_records, marker=marker
        )
        connection_list = [c.to_dict() for c in connections]
        # TODO: Add Marker (optional) to the response
        return json.dumps({"Connections": connection_list})
