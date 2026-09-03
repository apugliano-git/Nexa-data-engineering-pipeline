import re
import subprocess
import sys

from pyspark.sql import SparkSession


EXPECTED_SPARK = "3.5.8"
EXPECTED_PYTHON = (3, 10)
EXPECTED_MASTER = "local[2]"


def read_java_version():
    result = subprocess.run(
        ["java", "-version"],
        check=True,
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    match = re.search(r'version "([^"]+)"', output)
    if match is None:
        raise RuntimeError("No se pudo determinar la versión de Java")
    return match.group(1)


def main():
    java_version = read_java_version()
    if not java_version.startswith("17."):
        raise AssertionError(f"Java inesperado: {java_version}")
    if sys.version_info[:2] != EXPECTED_PYTHON:
        raise AssertionError(
            f"Python inesperado: {sys.version_info.major}.{sys.version_info.minor}"
        )

    spark = SparkSession.builder.appName("nexa-hito-1-smoke-test").getOrCreate()
    try:
        if spark.version != EXPECTED_SPARK:
            raise AssertionError(f"Spark inesperado: {spark.version}")
        if spark.sparkContext.master != EXPECTED_MASTER:
            raise AssertionError(
                f"Master inesperado: {spark.sparkContext.master}"
            )

        count = spark.range(1).count()
        if count != 1:
            raise AssertionError(f"Resultado inesperado: {count}")

        print(
            "NEXA_SMOKE_TEST_OK "
            f"spark={spark.version} "
            f"python={sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} "
            f"java={java_version} "
            f"master={spark.sparkContext.master} count={count}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
