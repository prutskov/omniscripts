import os
import re
import sys
import traceback

from environment import CondaEnvironment
from server import perform_ibis_tests
from utils import (
    create_cli,
    combinate_requirements,
)


def main():
    omniscript_path = os.path.dirname(__file__)
    supported_tasks = ["build", "test", "benchmark"]
    supported_benchmarks = ["ny_taxi", "santander", "census", "plasticc", "mortgage"]
    args = None

    interface = create_cli(omniscript_path, supported_tasks, supported_benchmarks)

    try:
        args = interface.parse_args()

        os.environ["IBIS_TEST_OMNISCIDB_DATABASE"] = args.database_name
        os.environ["IBIS_TEST_DATA_DB"] = args.database_name
        os.environ["PYTHONIOENCODING"] = "UTF-8"
        os.environ["PYTHONUNBUFFERED"] = "1"

        ibis_requirements = os.path.join(
            args.ibis_path, "ci", f"requirements-{args.python_version}-dev.yml"
        )
        requirements_file = "requirements.yml"

        conda_env = CondaEnvironment(args.env_name)

        print("PREPARING ENVIRONMENT")
        combinate_requirements(ibis_requirements, args.ci_requirements, requirements_file)
        conda_env.create(args.env_check, requirements_file=requirements_file)

        if "build" in args.tasks:
            install_ibis_cmdline = ["python3", os.path.join("setup.py"), "install"]

            print("IBIS INSTALLATION")
            conda_env.run(install_ibis_cmdline, cwd=args.ibis_path, print_output=False)

        if "test" in args.tasks:
            perform_ibis_tests(args, conda_env)

        if "benchmark" in args.tasks:
            perform_omniscript_benchmarks(args, conda_env, interface, omniscript_path)

    except Exception:
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    finally:
        if args and args.save_env is False:
            conda_env.remove()


def perform_omniscript_benchmarks(args, conda_env, interface, omniscript_path):
    benchmark_script_path = os.path.join(omniscript_path, "run_ibis_benchmark.py")

    benchmark_cmd = ["python3", benchmark_script_path]

    possible_benchmark_args = [
        "bench_name",
        "data_file",
        "dfiles_num",
        "iterations",
        "dnd",
        "dni",
        "validation",
        "optimizer",
        "no_ibis",
        "no_pandas",
        "pandas_mode",
        "ray_tmpdir",
        "ray_memory",
        "no_ml",
        "gpu_memory",
        "db_server",
        "db_port",
        "db_user",
        "db_pass",
        "db_name",
        "db_table_etl",
        "db_table_ml",
        "executable",
        "omnisci_cwd",
        "port",
        "http_port",
        "calcite_port",
        "user",
        "password",
        "ipc_conn",
        "database_name",
        "table",
        "commit_omnisci",
        "commit_ibis",
        "import_mode",
        "debug_timer",
        "columnar_output",
        "lazy_fetch",
        "multifrag_rs",
        "fragments_size",
        "omnisci_run_kwargs",
    ]
    args_dict = vars(args)
    for arg_name in list(interface._option_string_actions.keys()):
        try:
            pure_arg = re.sub(r"^--*", "", arg_name)
            if pure_arg in possible_benchmark_args:
                arg_value = args_dict[pure_arg]
                # correct filling of arguments with default values
                if arg_value is not None:
                    if isinstance(arg_value, dict):
                        if arg_value:
                            benchmark_cmd.extend(
                                [
                                    arg_name,
                                    ",".join(f"{key}={value}" for key, value in arg_value.items()),
                                ]
                            )
                    elif isinstance(arg_value, (list, tuple)):
                        if arg_value:
                            benchmark_cmd.extend([arg_name] + [str(x) for x in arg_value])
                    else:
                        benchmark_cmd.extend([arg_name, str(arg_value)])

        except KeyError:
            pass

    print(benchmark_cmd)
    conda_env.run(benchmark_cmd)


if __name__ == "__main__":
    main()
