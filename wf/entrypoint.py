from dataclasses import dataclass
from enum import Enum
import os
import subprocess
import requests
import shutil
from pathlib import Path
import typing
import typing_extensions

from latch.resources.workflow import workflow
from latch.resources.tasks import nextflow_runtime_task, custom_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir
from latch.ldata.path import LPath
from latch_cli.nextflow.workflow import get_flag
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.utils import urljoins
from latch.types import metadata
from flytekit.core.annotation import FlyteAnnotation

from latch_cli.services.register.utils import import_module_by_path

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)
import latch_metadata

@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {"Authorization": f"Latch-Execution-Token {token}"}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={
            "storage_gib": 100,
        }
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]






@nextflow_runtime_task(cpu=4, memory=8, storage_gib=100)
def nextflow_runtime(pvc_name: str, input: LatchFile, tools: typing.Optional[LatchFile], outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], email: typing.Optional[str], multiqc_title: typing.Optional[str], skip_stats: typing.Optional[bool], calc_sim: typing.Optional[bool], calc_seq_stats: typing.Optional[bool], extract_plddt: typing.Optional[bool], calc_gaps: typing.Optional[bool], skip_eval: typing.Optional[bool], calc_sp: typing.Optional[bool], calc_tc: typing.Optional[bool], calc_irmsd: typing.Optional[bool], calc_tcs: typing.Optional[bool], skip_multiqc: typing.Optional[bool], skip_shiny: typing.Optional[bool], shiny_app: typing.Optional[LatchDir], shiny_trace_mode: typing.Optional[str], no_compression: typing.Optional[bool], multiqc_methods_description: typing.Optional[str]) -> None:
    try:
        shared_dir = Path("/nf-workdir")



        ignore_list = [
            "latch",
            ".latch",
            "nextflow",
            ".nextflow",
            "work",
            "results",
            "miniconda",
            "anaconda3",
            "mambaforge",
        ]

        shutil.copytree(
            Path("/root"),
            shared_dir,
            ignore=lambda src, names: ignore_list,
            ignore_dangling_symlinks=True,
            dirs_exist_ok=True,
        )

        cmd = [
            "/root/nextflow",
            "run",
            str(shared_dir / "main.nf"),
            "-work-dir",
            str(shared_dir),
            "-profile",
            "docker",
            "-c",
            "latch.config",
                *get_flag('input', input),
                *get_flag('tools', tools),
                *get_flag('outdir', outdir),
                *get_flag('email', email),
                *get_flag('multiqc_title', multiqc_title),
                *get_flag('skip_stats', skip_stats),
                *get_flag('calc_sim', calc_sim),
                *get_flag('calc_seq_stats', calc_seq_stats),
                *get_flag('extract_plddt', extract_plddt),
                *get_flag('calc_gaps', calc_gaps),
                *get_flag('skip_eval', skip_eval),
                *get_flag('calc_sp', calc_sp),
                *get_flag('calc_tc', calc_tc),
                *get_flag('calc_irmsd', calc_irmsd),
                *get_flag('calc_tcs', calc_tcs),
                *get_flag('skip_multiqc', skip_multiqc),
                *get_flag('skip_shiny', skip_shiny),
                *get_flag('shiny_app', shiny_app),
                *get_flag('shiny_trace_mode', shiny_trace_mode),
                *get_flag('no_compression', no_compression),
                *get_flag('multiqc_methods_description', multiqc_methods_description)
        ]

        print("Launching Nextflow Runtime")
        print(' '.join(cmd))
        print(flush=True)

        env = {
            **os.environ,
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms2048M -Xmx8G -XX:ActiveProcessorCount=4",
            "K8S_STORAGE_CLAIM_NAME": pvc_name,
            "NXF_DISABLE_CHECK_LATEST": "true",
        }
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
    finally:
        print()

        nextflow_log = shared_dir / ".nextflow.log"
        if nextflow_log.exists():
            name = _get_execution_name()
            if name is None:
                print("Skipping logs upload, failed to get execution name")
            else:
                remote = LPath(urljoins("latch:///your_log_dir/nf_nf_core_multiplesequencealign", name, "nextflow.log"))
                print(f"Uploading .nextflow.log to {remote.path}")
                remote.upload_from(nextflow_log)



@workflow(metadata._nextflow_metadata)
def nf_nf_core_multiplesequencealign(input: LatchFile, tools: typing.Optional[LatchFile], outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], email: typing.Optional[str], multiqc_title: typing.Optional[str], skip_stats: typing.Optional[bool], calc_sim: typing.Optional[bool], calc_seq_stats: typing.Optional[bool], extract_plddt: typing.Optional[bool], calc_gaps: typing.Optional[bool], skip_eval: typing.Optional[bool], calc_sp: typing.Optional[bool], calc_tc: typing.Optional[bool], calc_irmsd: typing.Optional[bool], calc_tcs: typing.Optional[bool], skip_multiqc: typing.Optional[bool], skip_shiny: typing.Optional[bool], shiny_app: typing.Optional[LatchDir], shiny_trace_mode: typing.Optional[str], no_compression: typing.Optional[bool], multiqc_methods_description: typing.Optional[str]) -> None:
    """
    nf-core/multiplesequencealign

    Sample Description
    """

    pvc_name: str = initialize()
    nextflow_runtime(pvc_name=pvc_name, input=input, tools=tools, outdir=outdir, email=email, multiqc_title=multiqc_title, skip_stats=skip_stats, calc_sim=calc_sim, calc_seq_stats=calc_seq_stats, extract_plddt=extract_plddt, calc_gaps=calc_gaps, skip_eval=skip_eval, calc_sp=calc_sp, calc_tc=calc_tc, calc_irmsd=calc_irmsd, calc_tcs=calc_tcs, skip_multiqc=skip_multiqc, skip_shiny=skip_shiny, shiny_app=shiny_app, shiny_trace_mode=shiny_trace_mode, no_compression=no_compression, multiqc_methods_description=multiqc_methods_description)

