import linkml_runtime.processing.referencevalidator
import linkml_runtime.utils.schemaview

from ._globals import _QUEUE_CONFIG_SCHEMA_PATH


def _validate_queue_config(*, queue_config: dict) -> None:
    """Validate queue_config payload (top-level ``pipelines`` mapping) against the LinkML schema."""
    validator = linkml_runtime.processing.referencevalidator.ReferenceValidator(
        linkml_runtime.utils.schemaview.SchemaView(str(_QUEUE_CONFIG_SCHEMA_PATH))
    )
    report = validator.validate(queue_config, target="PipelinesConfig")
    errors = [result for result in report.results if not (result.normalized or result.repaired)]
    if errors:
        message = (
            f"Invalid queue configuration: LinkML validation failed with {len(errors)} error(s). "
            f"First error: {errors[0]!r}"
        )
        raise ValueError(message)
