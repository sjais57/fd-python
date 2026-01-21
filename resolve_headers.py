async def _resolve_headers(
    self,
    headers: Dict[str, Any],
    request_id: Optional[str] = None
) -> Dict[str, str]:
    """
    Resolve static + dynamic headers.
    Supports function-based headers like JWT dynamic claims.
    """
    resolved = {}

    for key, value in headers.items():
        # Static header
        if isinstance(value, str):
            resolved[key] = value
            continue

        # Dynamic function header
        if isinstance(value, dict) and value.get("type") == "function":
            module_path = value["module"]
            func_name = value["function"]
            args = value.get("args", {})
            prefix = value.get("prefix", "")

            logger.info(
                f"Resolving dynamic health header '{key}' via "
                f"{module_path}.{func_name}"
            )

            module = __import__(module_path, fromlist=[func_name])
            func = getattr(module, func_name)

            token = func(**args, request_id=request_id)
            resolved[key] = f"{prefix}{token}"
            continue

        raise ValueError(f"Invalid header definition for '{key}'")

    return resolved

_perform_check()
headers = await self._resolve_headers(
    target.headers,
    request_id=f"health-{target.name}"
)

request_kwargs = {
    "method": target.method,
    "url": target.url,
    "headers": headers,
    "timeout": target.timeout_seconds,
}

