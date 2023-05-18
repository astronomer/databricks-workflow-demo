"Defines a job cluster spec in one common place"

job_cluster_spec = [
    {
        "job_cluster_key": "astro_databricks",
        "new_cluster": {
            "cluster_name": "",
            "spark_version": "11.3.x-scala2.12",
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "zone_id": "us-east-2b",
                "spot_bid_price_percent": 100,
                "ebs_volume_count": 0,
            },
            "node_type_id": "i3.xlarge",
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "enable_elastic_disk": False,
            "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
            "runtime_engine": "STANDARD",
            "num_workers": 8,
        },
    }
]
