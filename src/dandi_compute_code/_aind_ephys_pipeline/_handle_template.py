import pathlib

import jinja2

from ._globals import _RAW_TEMPLATE_FILE_PATH


def generate_aind_ephys_submission_script(output_directory: pathlib.Path) -> None:
    raw_template = _RAW_TEMPLATE_FILE_PATH.read_text()

    template = jinja2.Template(source=raw_template)

    script = template.render(
        job_name="AIND-Ephys-Pipeline", memory="16GB", partition="mit_normal", blob_id="abc123", run_id="001"
    )

    code_dir = output_directory / "code"
    code_dir.mkdir(exist_ok=True)
    script_file_path = code_dir / "submit.sh"
    script_file_path.write_text(data=script)
