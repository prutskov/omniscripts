import argparse
import os

from .utils import find_free_port, KeyValueListParser, str_arg_to_bool


def add_common_arguments(parser, omniscript_path, supported_tasks):
    common = parser.add_argument_group("common")

    common.add_argument(
        "-tasks",
        dest="tasks",
        nargs="+",
        required=True,
        choices=supported_tasks,
        help="Task for execute.",
    )
    common.add_argument(
        "-en", "--env_name", dest="env_name", required=True, help="Conda env name."
    )
    common.add_argument(
        "-ec",
        "--env_check",
        dest="env_check",
        default=False,
        type=str_arg_to_bool,
        help="Check if env exists. If it exists don't recreate.",
    )
    common.add_argument(
        "-s",
        "--save_env",
        dest="save_env",
        default=False,
        type=str_arg_to_bool,
        help="Save conda env after executing.",
    )
    common.add_argument(
        "-r",
        "--report_path",
        dest="report_path",
        default=os.path.join(omniscript_path, ".."),
        help="Path to report file.",
    )
    common.add_argument(
        "-ci",
        "--ci_requirements",
        dest="ci_requirements",
        default=os.path.join(omniscript_path, "ci_requirements.yml"),
        help="File with ci requirements for conda env.",
    )
    common.add_argument(
        "-py",
        "--python_version",
        dest="python_version",
        default="3.7",
        choices=["3.6", "3.7"],
        help="File with ci requirements for conda env.",
    )


def add_mysql_arguments(parser):
    mysql = parser.add_argument_group("mysql")

    mysql.add_argument(
        "-db_server", dest="db_server", default="localhost", help="Host name of MySQL server."
    )
    mysql.add_argument(
        "-db_port", dest="db_port", default=3306, type=int, help="Port number of MySQL server."
    )
    mysql.add_argument(
        "-db_user",
        dest="db_user",
        help="Username to use to connect to MySQL database. "
        "If user name is specified, script attempts to store results in MySQL "
        "database using other -db-* parameters.",
    )
    mysql.add_argument(
        "-db_pass",
        dest="db_pass",
        default="omniscidb",
        help="Password to use to connect to MySQL database.",
    )
    mysql.add_argument(
        "-db_name",
        dest="db_name",
        default="omniscidb",
        help="MySQL database to use to store benchmark results.",
    )
    mysql.add_argument(
        "-db_table_etl",
        dest="db_table_etl",
        help="Table to use to store ETL results for this benchmark.",
    )
    mysql.add_argument(
        "-db_table_ml",
        dest="db_table_ml",
        help="Table to use to store ML results for this benchmark.",
    )


def add_omnisci_arguments(parser):
    omnisci = parser.add_argument_group("omnisci")

    omnisci.add_argument(
        "-executable", dest="executable", required=True, help="Path to omnisci_server executable."
    )
    omnisci.add_argument(
        "-omnisci_cwd",
        dest="omnisci_cwd",
        help="Path to omnisci working directory. "
        "By default parent directory of executable location is used. "
        "Data directory is used in this location.",
    )
    omnisci.add_argument(
        "-port",
        dest="port",
        default=find_free_port(),
        type=int,
        help="TCP port number to run omnisci_server on.",
    )
    omnisci.add_argument(
        "-http_port",
        dest="http_port",
        default=find_free_port(),
        type=int,
        help="HTTP port number to run omnisci_server on.",
    )
    omnisci.add_argument(
        "-calcite_port",
        dest="calcite_port",
        default=find_free_port(),
        type=int,
        help="Calcite port number to run omnisci_server on.",
    )
    omnisci.add_argument(
        "-user", dest="user", default="admin", help="User name to use on omniscidb server."
    )
    omnisci.add_argument(
        "-password",
        dest="password",
        default="HyperInteractive",
        help="User password to use on omniscidb server.",
    )
    omnisci.add_argument(
        "-database_name",
        dest="database_name",
        default="agent_test_ibis",
        help="Database name to use in omniscidb server.",
    )
    omnisci.add_argument(
        "-table",
        dest="table",
        default="benchmark_table",
        help="Table name name to use in omniscidb server.",
    )
    omnisci.add_argument(
        "-ipc_conn",
        dest="ipc_connection",
        default=True,
        type=str_arg_to_bool,
        help="Connection type for ETL operations",
    )
    omnisci.add_argument(
        "-debug_timer",
        dest="debug_timer",
        default=False,
        type=str_arg_to_bool,
        help="Enable fine-grained query execution timers for debug.",
    )
    omnisci.add_argument(
        "-columnar_output",
        dest="columnar_output",
        default=None,
        type=str_arg_to_bool,
        help="Allows OmniSci Core to directly materialize intermediate projections \
            and the final ResultSet in Columnar format where appropriate.",
    )
    omnisci.add_argument(
        "-lazy_fetch",
        dest="lazy_fetch",
        default=None,
        type=str_arg_to_bool,
        help="[lazy_fetch help message]",
    )
    omnisci.add_argument(
        "-multifrag_rs",
        dest="multifrag_rs",
        default=None,
        type=str_arg_to_bool,
        help="[multifrag_rs help message]",
    )
    omnisci.add_argument(
        "-fragments_size",
        dest="fragments_size",
        default=None,
        nargs="*",
        type=int,
        help="Number of rows per fragment that is a unit of the table for query processing. \
            Should be specified for each table in workload",
    )
    omnisci.add_argument(
        "-omnisci_run_kwargs",
        dest="omnisci_run_kwargs",
        default={},
        metavar="KEY1=VAL1,KEY2=VAL2...",
        action=KeyValueListParser,
        help="additional options to pass when starting omnisci server",
    )
    omnisci.add_argument(
        "-commit_omnisci",
        dest="commit_omnisci",
        default="1234567890123456789012345678901234567890",
        help="Omnisci commit hash to use for tests.",
    )


def add_ibis_arguments(parser):
    ibis = parser.add_argument_group("ibis")

    ibis.add_argument(
        "-i", "--ibis_path", dest="ibis_path", required=True, help="Path to ibis directory."
    )
    ibis.add_argument(
        "-expression",
        dest="expression",
        default=" ",
        help="Run tests which match the given substring test names and their parent "
        "classes. Example: 'test_other', while 'not test_method' matches those "
        "that don't contain 'test_method' in their names.",
    )
    ibis.add_argument(
        "-commit_ibis",
        dest="commit_ibis",
        default="1234567890123456789012345678901234567890",
        help="Ibis commit hash to use for tests.",
    )


def add_benchmark_arguments(parser, supported_benchmarks):
    benchmark = parser.add_argument_group("benchmark")

    benchmark.add_argument(
        "-bench_name", dest="bench_name", choices=supported_benchmarks, help="Benchmark name."
    )
    benchmark.add_argument(
        "-data_file", dest="data_file", help="A datafile that should be loaded."
    )
    benchmark.add_argument(
        "-dfiles_num",
        dest="dfiles_num",
        default=1,
        type=int,
        help="Number of datafiles to input into database for processing.",
    )
    benchmark.add_argument(
        "-iterations",
        dest="iterations",
        default=1,
        type=int,
        help="Number of iterations to run every query. Best result is selected.",
    )
    benchmark.add_argument(
        "-dnd", default=False, type=str_arg_to_bool, help="Do not delete old table."
    )
    benchmark.add_argument(
        "-dni",
        default=False,
        type=str_arg_to_bool,
        help="Do not create new table and import any data from CSV files.",
    )
    benchmark.add_argument(
        "-validation",
        dest="validation",
        default=False,
        type=str_arg_to_bool,
        help="validate queries results (by comparison with Pandas queries results).",
    )
    benchmark.add_argument(
        "-import_mode",
        dest="import_mode",
        default="fsi",
        choices=["copy-from", "pandas", "fsi"],
        help="mode to read datasets",
    )
    benchmark.add_argument(
        "-optimizer",
        choices=["intel", "stock"],
        dest="optimizer",
        default="intel",
        help="Which optimizer to use",
    )
    benchmark.add_argument(
        "-no_ibis",
        default=False,
        type=str_arg_to_bool,
        help="Do not run Ibis benchmark, run only Pandas (or Modin) version",
    )
    benchmark.add_argument(
        "-no_pandas",
        default=False,
        type=str_arg_to_bool,
        help="Do not run Pandas version of benchmark",
    )
    benchmark.add_argument(
        "-pandas_mode",
        choices=["Pandas", "Modin_on_ray", "Modin_on_dask", "Modin_on_python"],
        default="Pandas",
        help="Specifies which version of Pandas to use.",
    )
    benchmark.add_argument(
        "-ray_tmpdir",
        default="/tmp",
        help="Location where to keep Ray plasma store. "
        "It should have enough space to keep -ray_memory",
    )
    benchmark.add_argument(
        "-ray_memory",
        default=200 * 1024 * 1024 * 1024,
        help="Size of memory to allocate for Ray plasma store",
    )
    benchmark.add_argument(
        "-no_ml",
        default=False,
        type=str_arg_to_bool,
        help="Do not run machine learning part, run only ETL part",
    )
    benchmark.add_argument(
        "-gpu_memory",
        dest="gpu_memory",
        type=int,
        help="specify the memory of your gpu, default 16. "
        "(This controls the amount of lines to be used. Also work for CPU version. )",
        default=16,
    )


def create_cli(omniscript_path, supported_tasks, supported_benchmarks):
    parser = argparse.ArgumentParser(description="Run internal tests from ibis project")

    add_common_arguments(parser, omniscript_path, supported_tasks)
    add_omnisci_arguments(parser)
    add_ibis_arguments(parser)
    add_benchmark_arguments(parser, supported_benchmarks)
    add_mysql_arguments(parser)

    return parser
