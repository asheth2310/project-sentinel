"""Payload validation pipeline for the ingestion gateway.

Provides a custom exception handler that transforms Pydantic's
RequestValidationError into structured ErrorResponse payloads (HTTP 422)
with field-level error details.

Validates: Requirement 1.4, 2.1-2.7
"""

from uuid import UUID

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src.models.responses import ErrorResponse, ValidationErrorDetail


def _format_field_path(loc: tuple) -> str:
    """Convert Pydantic error location tuple into a dot-separated field path.

    Examples:
        ("body", "events", 0, "prompt_tokens") -> "events[0].prompt_tokens"
        ("body", "sdk_version") -> "sdk_version"
    """
    parts: list[str] = []
    for segment in loc:
        # Skip 'body' prefix added by FastAPI
        if segment == "body":
            continue
        if isinstance(segment, int):
            # Array index: attach to previous part
            if parts:
                parts[-1] = f"{parts[-1]}[{segment}]"
            else:
                parts.append(f"[{segment}]")
        else:
            parts.append(str(segment))
    return ".".join(parts) if parts else "body"


def _build_validation_error_details(
    errors: list[dict],
) -> list[ValidationErrorDetail]:
    """Transform Pydantic validation errors into ValidationErrorDetail list."""
    details: list[ValidationErrorDetail] = []
    for error in errors:
        loc = error.get("loc", ())
        field_path = _format_field_path(tuple(loc))
        message = error.get("msg", "Validation error")
        error_type = error.get("type", "validation_error")
        details.append(
            ValidationErrorDetail(
                field=field_path,
                message=message,
                type=error_type,
            )
        )
    return details


def _extract_request_id(request: Request) -> UUID | None:
    """Extract request_id from request headers or state if available."""
    # Check X-Request-ID header first
    request_id_header = request.headers.get("x-request-id")
    if request_id_header:
        try:
            return UUID(request_id_header)
        except (ValueError, AttributeError):
            pass

    # Check request state (may be set by middleware)
    request_id = getattr(request.state, "request_id", None)
    if isinstance(request_id, UUID):
        return request_id

    return None


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Custom exception handler for RequestValidationError.

    Transforms Pydantic validation errors into our structured ErrorResponse
    format with field-level error details, returning HTTP 422.

    Args:
        request: The incoming HTTP request.
        exc: The validation exception raised by FastAPI/Pydantic.

    Returns:
        JSONResponse with 422 status and ErrorResponse body containing
        all field-level validation errors.
    """
    errors = exc.errors()
    error_details = _build_validation_error_details(errors)
    request_id = _extract_request_id(request)

    error_count = len(error_details)
    detail_message = (
        f"Validation failed with {error_count} error(s)"
        if error_count > 1
        else "Validation failed"
    )

    error_response = ErrorResponse(
        detail=detail_message,
        errors=error_details,
        request_id=request_id,
    )

    return JSONResponse(
        status_code=422,
        content=error_response.model_dump(mode="json"),
    )


def register_validation_handler(app) -> None:
    """Register the custom validation exception handler on the FastAPI app.

    Call this during app startup to override FastAPI's default 422 response
    format with our structured ErrorResponse.

    Args:
        app: The FastAPI application instance.
    """
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
