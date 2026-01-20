"""
Health Check Service for DSP-FD2

Features:
- Manifest-driven health checks
- Dynamic headers (function-based, e.g. OAuth token)
- Liveness & readiness probes
- Retry + caching
- Parallel execution
"""

import asyncio
import logging
import time
import re
import importlib
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger("DSP-FD2.HealthCheck")


# =========================
# Enums & Data Models
# =========================

class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    name: str
    status: HealthStatus
    response_time_ms: float
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    critical: bool = True


@dataclass
class HealthCheckTargetConfig:
    name: str
    url: str
    method: str = "GET"

    # 🔴 MUST be Any (supports dict for dynamic headers)
    headers: Dict[str, Any] = field(default_factory=dict)

    payload: Optional[Dict[str, Any]] = None
    timeout_seconds: int = 10
    expected_status_codes: List[int] = field(default_factory=lambda: [200])

    response_json_path: Optional[str] = None
    expected_value: Optional[Any] = None
    response_contains: Optional[str] = None
    response_not_contains: Optional[str] = None

    critical: bool = True
    retry_count: int = 1
    retry_delay_seconds: float = 1.0


@dataclass
class HealthCheckConfig:
    enabled: bool = True
    endpoint_path: str = "/health"
    targets: List[HealthCheckTargetConfig] = field(default_factory=list)

    liveness_path: str = "/health/live"
    readiness_path: str = "/health/ready"

    check_interval_seconds: int = 30
    cache_results_seconds: int = 5

    parallel_checks: bool = True
    include_details: bool = True

    failure_threshold: int = 3
    success_threshold: int = 1

    require_auth: bool = False
    auth_module_reference: Optional[str] = None
    response_format: str = "detailed"


# =========================
# Health Check Service
# =========================

class HealthCheckService:
    def __init__(self, config: Optional[HealthCheckConfig] = None):
        self.config = config or HealthCheckConfig()
        self.http_client: Optional[httpx.AsyncClient] = None

        self._cache: Dict[str, Tuple[datetime, HealthCheckResult]] = {}
        self._last_check_time: Optional[datetime] = None
        self._cached_overall_result: Optional[Dict[str, Any]] = None

        self._failure_counts: Dict[str, int] = {}
        self._success_counts: Dict[str, int] = {}

        logger.info(
            "HealthCheckService initialized with %d targets",
            len(self.config.targets),
        )

    # -------------------------
    # Lifecycle
    # -------------------------

    async def initialize(self):
        if not self.http_client:
            self.http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                follow_redirects=True,
            )

    async def shutdown(self):
        if self.http_client:
            await self.http_client.aclose()
            self.http_client = None

    # -------------------------
    # Factory
    # -------------------------

    @classmethod
    def from_manifest_config(cls, manifest_config: Dict[str, Any]) -> "HealthCheckService":
        """
        IMPORTANT:
        Caller MUST pass module["config"], not the full module.
        """
        if "targets" not in manifest_config:
            logger.error(
                "Health manifest missing 'targets'. Keys received: %s",
                list(manifest_config.keys()),
            )

        targets: List[HealthCheckTargetConfig] = []

        for target_data in manifest_config.get("targets", []):
            targets.append(
                HealthCheckTargetConfig(
                    name=target_data.get("name", "unknown"),
                    url=target_data.get("url", ""),
                    method=target_data.get("method", "GET"),
                    headers=target_data.get("headers", {}),
                    payload=target_data.get("payload"),
                    timeout_seconds=target_data.get("timeout_seconds", 10),
                    expected_status_codes=target_data.get(
                        "expected_status_codes", [200]
                    ),
                    response_json_path=target_data.get("response_json_path"),
                    expected_value=target_data.get("expected_value"),
                    response_contains=target_data.get("response_contains"),
                    response_not_contains=target_data.get("response_not_contains"),
                    critical=target_data.get("critical", True),
                    retry_count=target_data.get("retry_count", 1),
                    retry_delay_seconds=target_data.get("retry_delay_seconds", 1.0),
                )
            )

        config = HealthCheckConfig(
            enabled=manifest_config.get("enabled", True),
            endpoint_path=manifest_config.get("endpoint_path", "/health"),
            targets=targets,
            liveness_path=manifest_config.get("liveness_path", "/health/live"),
            readiness_path=manifest_config.get("readiness_path", "/health/ready"),
            check_interval_seconds=manifest_config.get(
                "check_interval_seconds", 30
            ),
            cache_results_seconds=manifest_config.get(
                "cache_results_seconds", 5
            ),
            parallel_checks=manifest_config.get("parallel_checks", True),
            include_details=manifest_config.get("include_details", True),
            failure_threshold=manifest_config.get("failure_threshold", 3),
            success_threshold=manifest_config.get("success_threshold", 1),
            require_auth=manifest_config.get("require_auth", False),
            auth_module_reference=manifest_config.get("auth_module_reference"),
            response_format=manifest_config.get("response_format", "detailed"),
        )

        logger.info(
            "Loaded %d health targets: %s",
            len(targets),
            [t.name for t in targets],
        )

        return cls(config)

    # -------------------------
    # Header Resolution
    # -------------------------

    async def _resolve_headers(self, headers: Dict[str, Any]) -> Dict[str, str]:
        """
        Supports:
        - Static headers
        - Function-based headers:
          {
            "Authorization": {
              "type": "function",
              "module": "utils.api_key",
              "function": "get_fedsso_auth_token",
              "args": {...},
              "prefix": "Bearer "
            }
          }
        """
        resolved: Dict[str, str] = {}

        for name, value in headers.items():

            # Static header
            if isinstance(value, str):
                resolved[name] = value
                continue

            # Dynamic function header
            if isinstance(value, dict) and value.get("type") == "function":
                module_name = value.get("module")
                function_name = value.get("function")
                args = value.get("args", {})
                prefix = value.get("prefix", "")

                if not module_name or not function_name:
                    raise ValueError(f"Invalid header config for {name}")

                module = importlib.import_module(module_name)
                func = getattr(module, function_name)

                # Run sync function safely in thread
                token = await asyncio.to_thread(func, **args)

                if not token:
                    raise RuntimeError(
                        f"Token generation failed for header {name}"
                    )

                resolved[name] = f"{prefix}{token}"
                continue

            raise ValueError(f"Unsupported header format for {name}: {value}")

        return resolved

    # -------------------------
    # Target Execution
    # -------------------------

    async def _perform_check(
        self, target: HealthCheckTargetConfig
    ) -> HealthCheckResult:
        await self.initialize()

        resolved_headers = await self._resolve_headers(target.headers)

        request_kwargs = {
            "method": target.method,
            "url": target.url,
            "headers": resolved_headers,
            "timeout": target.timeout_seconds,
        }

        if target.payload and target.method.upper() in ("POST", "PUT", "PATCH"):
            request_kwargs["json"] = target.payload

        response = await self.http_client.request(**request_kwargs)

        if response.status_code not in target.expected_status_codes:
            return HealthCheckResult(
                name=target.name,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=0,
                message=f"Unexpected status {response.status_code}",
                details={"status_code": response.status_code},
                critical=target.critical,
            )

        try:
            response_text = response.text
            response_json = (
                response.json()
                if response.headers.get("content-type", "").startswith("application/json")
                else None
            )
        except Exception:
            response_text = ""
            response_json = None

        if target.response_contains and target.response_contains not in response_text:
            return HealthCheckResult(
                name=target.name,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=0,
                message="Response missing expected content",
                critical=target.critical,
            )

        if target.response_not_contains and target.response_not_contains in response_text:
            return HealthCheckResult(
                name=target.name,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=0,
                message="Response contains forbidden content",
                critical=target.critical,
            )

        if target.response_json_path and response_json is not None:
            extracted = self._extract_json_path(
                response_json, target.response_json_path
            )
            if (
                target.expected_value is not None
                and extracted != target.expected_value
            ):
                return HealthCheckResult(
                    name=target.name,
                    status=HealthStatus.UNHEALTHY,
                    response_time_ms=0,
                    message="JSON path value mismatch",
                    details={
                        "expected": target.expected_value,
                        "actual": extracted,
                    },
                    critical=target.critical,
                )

        return HealthCheckResult(
            name=target.name,
            status=HealthStatus.HEALTHY,
            response_time_ms=0,
            message="OK",
            details={"status_code": response.status_code},
            critical=target.critical,
        )

    # -------------------------
    # JSON Path
    # -------------------------

    def _extract_json_path(self, data: Any, path: str) -> Any:
        if path.startswith("$."):
            path = path[2:]
        elif path.startswith("$"):
            path = path[1:]

        current = data
        parts = re.split(r"\.(?![^\[]*\])", path)

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None

        return current

    # -------------------------
    # Public APIs
    # -------------------------

    async def check_all(self) -> Dict[str, Any]:
        if not self.config.enabled:
            return {
                "status": HealthStatus.HEALTHY.value,
                "message": "Health checks disabled",
                "timestamp": datetime.utcnow().isoformat(),
            }

        if not self.config.targets:
            logger.error("HealthCheckService has ZERO targets configured")
            return {
                "status": HealthStatus.HEALTHY.value,
                "message": "No health check targets configured",
                "timestamp": datetime.utcnow().isoformat(),
                "checks": [],
            }

        results = await asyncio.gather(
            *[self.check_target(t) for t in self.config.targets],
            return_exceptions=True,
        )

        check_results: List[HealthCheckResult] = []

        for i, r in enumerate(results):
            if isinstance(r, Exception):
                check_results.append(
                    HealthCheckResult(
                        name=self.config.targets[i].name,
                        status=HealthStatus.UNHEALTHY,
                        response_time_ms=0,
                        message=str(r),
                        critical=self.config.targets[i].critical,
                    )
                )
            else:
                check_results.append(r)

        critical_failures = [
            r for r in check_results if r.status != HealthStatus.HEALTHY and r.critical
        ]
        non_critical_failures = [
            r for r in check_results if r.status != HealthStatus.HEALTHY and not r.critical
        ]

        if critical_failures:
            overall = HealthStatus.UNHEALTHY
        elif non_critical_failures:
            overall = HealthStatus.DEGRADED
        else:
            overall = HealthStatus.HEALTHY

        response = {
            "status": overall.value,
            "timestamp": datetime.utcnow().isoformat(),
            "total_checks": len(check_results),
            "healthy_checks": len(
                [r for r in check_results if r.status == HealthStatus.HEALTHY]
            ),
            "unhealthy_checks": len(
                [r for r in check_results if r.status != HealthStatus.HEALTHY]
            ),
        }

        if self.config.include_details:
            response["checks"] = [
                {
                    "name": r.name,
                    "status": r.status.value,
                    "response_time_ms": round(r.response_time_ms, 2),
                    "message": r.message,
                    "critical": r.critical,
                    "details": r.details,
                    "timestamp": r.timestamp.isoformat(),
                }
                for r in check_results
            ]

        return response

    async def check_target(self, target: HealthCheckTargetConfig) -> HealthCheckResult:
        start = time.perf_counter()
        result = await self._perform_check(target)
        result.response_time_ms = (time.perf_counter() - start) * 1000
        return result

    async def liveness_check(self) -> Dict[str, Any]:
        return {
            "status": HealthStatus.HEALTHY.value,
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Service is alive",
        }

    async def readiness_check(self) -> Dict[str, Any]:
        return await self.check_all()
