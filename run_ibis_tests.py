import os
import re
import sys
import traceback

from environment import CondaEnvironment
from server import OmnisciServer
from utils import (
    create_interface,
    combinate_requirements,
)


def main():
    omniscript_path = os.path.dirname(__file__)
    supported_tasks = ["build", "test", "benchmark"]
    supported_benchmarks = ["ny_taxi", "santander", "census", "plasticc", "mortgage"]
    omnisci_server = None
    args = None

    interface = create_interface(omniscript_path, supported_tasks, supported_benchmarks)

    try:
        args = interface.parse_args()

        os.environ["IBIS_TEST_OMNISCIDB_DATABASE"] = args.database_name
        os.environ["IBIS_TEST_DATA_DB"] = args.database_name
        os.environ["IBIS_TEST_OMNISCIDB_PORT"] = str(args.port)
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

        if "build" in args.task:
            install_ibis_cmdline = ["python3", os.path.join("setup.py"), "install"]

            print("IBIS INSTALLATION")
            conda_env.run(install_ibis_cmdline, cwd=args.ibis_path, print_output=False)

        if "test" in args.task:
            ibis_data_script = os.path.join(args.ibis_path, "ci", "datamgr.py")
            dataset_download_cmdline = ["python3", ibis_data_script, "download"]
            dataset_import_cmdline = [
                "python3",
                ibis_data_script,
                "omniscidb",
                "-P",
                str(args.port),
                "--database",
                args.database_name,
            ]
            report_file_name = f"report-{args.commit_ibis[:8]}-{args.commit_omnisci[:8]}.html"
            if not os.path.isdir(args.report_path):
                os.makedirs(args.report_path)
            report_file_path = os.path.join(args.report_path, report_file_name)

            ibis_tests_cmdline = [
                "pytest",
                "-m",
                "omniscidb",
                "--disable-pytest-warnings",
                "-k",
                args.expression,
                f"--html={report_file_path}",
            ]

            print("STARTING OMNISCI SERVER")
            omnisci_server = OmnisciServer(
                omnisci_executable=args.executable,
                omnisci_port=args.port,
                http_port=args.http_port,
                calcite_port=args.calcite_port,
                database_name=args.database_name,
                omnisci_cwd=args.omnisci_cwd,
                user=args.user,
                password=args.password,
                debug_timer=args.debug_timer,
                columnar_output=args.columnar_output,
                lazy_fetch=args.lazy_fetch,
                multifrag_rs=args.multifrag_rs,
                omnisci_run_kwargs=args.omnisci_run_kwargs,
            )
            omnisci_server.launch()

            print("PREPARING DATA")
            conda_env.run(dataset_download_cmdline)
            conda_env.run(dataset_import_cmdline)

            print("RUNNING TESTS")
            conda_env.run(ibis_tests_cmdline, cwd=args.ibis_path)

        if "benchmark" in args.task:
            if not args.data_file:
                print(
                    f"Parameter --data_file was received empty, but it is required for benchmarks"
                )
                sys.exit(1)

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
                "ipc_connection",
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
            args_dict["data_file"] = f"'{args_dict['data_file']}'"
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
                                            ",".join(
                                                f"{key}={value}"
                                                for key, value in arg_value.items()
                                            ),
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

    except Exception:
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    finally:
        if omnisci_server:
            omnisci_server.terminate()
        if args and args.save_env is False:
            conda_env.remove()


if __name__ == "__main__":
    main()
