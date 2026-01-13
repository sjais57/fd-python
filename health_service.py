"""
Health Check Service for DSP-FD2

Provides configurable health checks with:
- Route checking with sample payloads
- Response assertions (status codes, JSON path, contains)
- Liveness and readiness probes
- Caching and parallel execution
"""

import asyncio
import logging
import time
import re
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger("DSP-FD2.HealthCheck")


class HealthStatus(str, Enum):
    """Health status values"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a single health check"""
    name: str
    status: HealthStatus
    response_time_ms: float
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    critical: bool = True


@dataclass
class HealthCheckTargetConfig:
    """Configuration for a health check target"""
    name: str
    url: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
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
    """Health check module configuration"""
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


class HealthCheckService:
    """
    Service for performing health checks based on manifest configuration.
    
    Features:
    - Configurable health check targets with URL, method, headers, payload
    - Response assertions: status codes, JSON path extraction, contains checks
    - Retry logic with configurable delays
    - Result caching to prevent excessive checks
    - Parallel or sequential execution
    - Liveness (basic) and readiness (full) probe support
    """
    
    def __init__(self, config: Optional[HealthCheckConfig] = None):
        self.config = config or HealthCheckConfig()
        self.http_client: Optional[httpx.AsyncClient] = None
        self._cache: Dict[str, Tuple[datetime, HealthCheckResult]] = {}
        self._failure_counts: Dict[str, int] = {}
        self._success_counts: Dict[str, int] = {}
        self._last_check_time: Optional[datetime] = None
        self._cached_overall_result: Optional[Dict[str, Any]] = None
        
    async def initialize(self):
        """Initialize the HTTP client"""
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            follow_redirects=True
        )
        logger.info("HealthCheckService initialized")
        
    async def shutdown(self):
        """Cleanup resources"""
        if self.http_client:
            await self.http_client.aclose()
            self.http_client = None
        logger.info("HealthCheckService shutdown")
    
    def update_config(self, config: HealthCheckConfig):
        """Update the health check configuration"""
        self.config = config
        self._cache.clear()
        logger.info(f"HealthCheckService config updated with {len(config.targets)} targets")
    
    @classmethod
    def from_manifest_config(cls, manifest_config: Dict[str, Any]) -> "HealthCheckService":
        """Create service from manifest configuration dict"""
        targets = []
        for target_data in manifest_config.get("targets", []):
            targets.append(HealthCheckTargetConfig(
                name=target_data.get("name", "unknown"),
                url=target_data.get("url", ""),
                method=target_data.get("method", "GET"),
                headers=target_data.get("headers", {}),
                payload=target_data.get("payload"),
                timeout_seconds=target_data.get("timeout_seconds", 10),
                expected_status_codes=target_data.get("expected_status_codes", [200]),
                response_json_path=target_data.get("response_json_path"),
                expected_value=target_data.get("expected_value"),
                response_contains=target_data.get("response_contains"),
                response_not_contains=target_data.get("response_not_contains"),
                critical=target_data.get("critical", True),
                retry_count=target_data.get("retry_count", 1),
                retry_delay_seconds=target_data.get("retry_delay_seconds", 1.0),
            ))
        
        config = HealthCheckConfig(
            enabled=manifest_config.get("enabled", True),
            endpoint_path=manifest_config.get("endpoint_path", "/health"),
            targets=targets,
            liveness_path=manifest_config.get("liveness_path", "/health/live"),
            readiness_path=manifest_config.get("readiness_path", "/health/ready"),
            check_interval_seconds=manifest_config.get("check_interval_seconds", 30),
            cache_results_seconds=manifest_config.get("cache_results_seconds", 5),
            parallel_checks=manifest_config.get("parallel_checks", True),
            include_details=manifest_config.get("include_details", True),
            failure_threshold=manifest_config.get("failure_threshold", 3),
            success_threshold=manifest_config.get("success_threshold", 1),
            require_auth=manifest_config.get("require_auth", False),
            auth_module_reference=manifest_config.get("auth_module_reference"),
            response_format=manifest_config.get("response_format", "detailed"),
        )
        
        return cls(config)
    
    async def check_target(self, target: HealthCheckTargetConfig) -> HealthCheckResult:
        """
        Perform health check on a single target with retry logic.
        """
        if not self.http_client:
            await self.initialize()
        
        # Check cache
        cache_key = target.name
        if cache_key in self._cache:
            cached_time, cached_result = self._cache[cache_key]
            if datetime.utcnow() - cached_time < timedelta(seconds=self.config.cache_results_seconds):
                return cached_result
        
        last_error = None
        for attempt in range(target.retry_count):
            if attempt > 0:
                await asyncio.sleep(target.retry_delay_seconds)
            
            start_time = time.perf_counter()
            try:
                result = await self._perform_check(target)
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                result.response_time_ms = elapsed_ms
                
                # Update success/failure counts
                if result.status == HealthStatus.HEALTHY:
                    self._success_counts[target.name] = self._success_counts.get(target.name, 0) + 1
                    self._failure_counts[target.name] = 0
                else:
                    self._failure_counts[target.name] = self._failure_counts.get(target.name, 0) + 1
                    self._success_counts[target.name] = 0
                
                # Cache result
                self._cache[cache_key] = (datetime.utcnow(), result)
                return result
                
            except Exception as e:
                last_error = str(e)
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                logger.warning(f"Health check failed for {target.name} (attempt {attempt + 1}): {e}")
        
        # All retries failed
        self._failure_counts[target.name] = self._failure_counts.get(target.name, 0) + 1
        self._success_counts[target.name] = 0
        
        result = HealthCheckResult(
            name=target.name,
            status=HealthStatus.UNHEALTHY,
            response_time_ms=elapsed_ms,
            message=f"All {target.retry_count} attempts failed: {last_error}",
            critical=target.critical,
        )
        self._cache[cache_key] = (datetime.utcnow(), result)
        return result
    
    async def _perform_check(self, target: HealthCheckTargetConfig) -> HealthCheckResult:
        """Perform a single health check attempt"""
        # Build request
        request_kwargs = {
            "method": target.method,
            "url": target.url,
            "headers": target.headers,
            "timeout": target.timeout_seconds,
        }
        
        if target.payload and target.method.upper() in ("POST", "PUT", "PATCH"):
            request_kwargs["json"] = target.payload
        
        # Make request
        response = await self.http_client.request(**request_kwargs)
        
        # Check status code
        if response.status_code not in target.expected_status_codes:
            return HealthCheckResult(
                name=target.name,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=0,
                message=f"Unexpected status code: {response.status_code} (expected: {target.expected_status_codes})",
                details={"status_code": response.status_code},
                critical=target.critical,
            )
        
        # Get response body
        try:
            response_text = response.text
            response_json = response.json() if response.headers.get("content-type", "").startswith("application/json") else None
        except Exception:
            response_text = ""
            response_json = None
        
        # Check response_contains
        if target.response_contains:
            if target.response_contains not in response_text:
                return HealthCheckResult(
                    name=target.name,
                    status=HealthStatus.UNHEALTHY,
                    response_time_ms=0,
                    message=f"Response does not contain expected string: '{target.response_contains}'",
                    critical=target.critical,
                )
        
        # Check response_not_contains
        if target.response_not_contains:
            if target.response_not_contains in response_text:
                return HealthCheckResult(
                    name=target.name,
                    status=HealthStatus.UNHEALTHY,
                    response_time_ms=0,
                    message=f"Response contains forbidden string: '{target.response_not_contains}'",
                    critical=target.critical,
                )
        
        # Check JSON path
        if target.response_json_path and response_json is not None:
            extracted_value = self._extract_json_path(response_json, target.response_json_path)
            
            if target.expected_value is not None:
                if extracted_value != target.expected_value:
                    return HealthCheckResult(
                        name=target.name,
                        status=HealthStatus.UNHEALTHY,
                        response_time_ms=0,
                        message=f"JSON path '{target.response_json_path}' value mismatch: got '{extracted_value}', expected '{target.expected_value}'",
                        details={"extracted_value": extracted_value, "expected_value": target.expected_value},
                        critical=target.critical,
                    )
        
        # All checks passed
        return HealthCheckResult(
            name=target.name,
            status=HealthStatus.HEALTHY,
            response_time_ms=0,
            message="OK",
            details={"status_code": response.status_code},
            critical=target.critical,
        )
    
    def _extract_json_path(self, data: Any, path: str) -> Any:
        """
        Extract value from JSON using simple path notation.
        Supports: $.key, $.key.nested, $.array[0], $.key[*].field
        """
        if not path:
            return data
        
        # Remove leading $. if present
        if path.startswith("$."):
            path = path[2:]
        elif path.startswith("$"):
            path = path[1:]
        
        current = data
        # Split by . but handle array notation
        parts = re.split(r'\.(?![^\[]*\])', path)
        
        for part in parts:
            if not part:
                continue
                
            # Handle array notation like key[0] or key[*]
            array_match = re.match(r'(\w+)\[(\d+|\*)\]', part)
            if array_match:
                key, index = array_match.groups()
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
                    
                if isinstance(current, list):
                    if index == "*":
                        # Return all items (for simple cases)
                        continue
                    else:
                        idx = int(index)
                        if 0 <= idx < len(current):
                            current = current[idx]
                        else:
                            return None
            else:
                # Simple key access
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return None
        
        return current
    
    async def check_all(self) -> Dict[str, Any]:
        """
        Perform all configured health checks.
        Returns overall health status and individual check results.
        """
        if not self.config.enabled:
            return {
                "status": HealthStatus.HEALTHY.value,
                "message": "Health checks disabled",
                "timestamp": datetime.utcnow().isoformat(),
            }
        
        # Check cache for overall result
        if self._cached_overall_result and self._last_check_time:
            if datetime.utcnow() - self._last_check_time < timedelta(seconds=self.config.cache_results_seconds):
                return self._cached_overall_result
        
        if not self.config.targets:
            result = {
                "status": HealthStatus.HEALTHY.value,
                "message": "No health check targets configured",
                "timestamp": datetime.utcnow().isoformat(),
                "checks": [],
            }
            self._cached_overall_result = result
            self._last_check_time = datetime.utcnow()
            return result
        
        # Run checks
        if self.config.parallel_checks:
            results = await asyncio.gather(
                *[self.check_target(target) for target in self.config.targets],
                return_exceptions=True
            )
            # Handle exceptions
            check_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    check_results.append(HealthCheckResult(
                        name=self.config.targets[i].name,
                        status=HealthStatus.UNHEALTHY,
                        response_time_ms=0,
                        message=str(result),
                        critical=self.config.targets[i].critical,
                    ))
                else:
                    check_results.append(result)
        else:
            check_results = []
            for target in self.config.targets:
                result = await self.check_target(target)
                check_results.append(result)
        
        # Determine overall status
        critical_failures = [r for r in check_results if r.status != HealthStatus.HEALTHY and r.critical]
        non_critical_failures = [r for r in check_results if r.status != HealthStatus.HEALTHY and not r.critical]
        
        if critical_failures:
            overall_status = HealthStatus.UNHEALTHY
        elif non_critical_failures:
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.HEALTHY
        
        # Build response
        response = {
            "status": overall_status.value,
            "timestamp": datetime.utcnow().isoformat(),
            "total_checks": len(check_results),
            "healthy_checks": len([r for r in check_results if r.status == HealthStatus.HEALTHY]),
            "unhealthy_checks": len([r for r in check_results if r.status != HealthStatus.HEALTHY]),
        }
        
        if self.config.include_details or self.config.response_format == "detailed":
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
        
        self._cached_overall_result = response
        self._last_check_time = datetime.utcnow()
        return response
    
    async def liveness_check(self) -> Dict[str, Any]:
        """
        Simple liveness check - just confirms the service is running.
        Does not check dependencies.
        """
        return {
            "status": HealthStatus.HEALTHY.value,
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Service is alive",
        }
    
    async def readiness_check(self) -> Dict[str, Any]:
        """
        Full readiness check - checks all dependencies.
        """
        return await self.check_all()
    
    def get_failure_counts(self) -> Dict[str, int]:
        """Get current failure counts for all targets"""
        return dict(self._failure_counts)
    
    def get_success_counts(self) -> Dict[str, int]:
        """Get current success counts for all targets"""
        return dict(self._success_counts)
