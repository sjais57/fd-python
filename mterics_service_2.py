"""
Prometheus Metrics Service for DSP-FD2

Provides Prometheus-compatible metrics with:
- Default metrics (request count, latency, errors, etc.)
- Custom metric definitions from manifest
- Histogram and summary support
- Label management
- Optional Pushgateway integration
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from enum import Enum

try:
    from prometheus_client import (
        Counter, Gauge, Histogram, Summary,
        CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST,
        push_to_gateway, REGISTRY
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    CollectorRegistry = None
    REGISTRY = None

logger = logging.getLogger("DSP-FD2.Metrics")


class MetricType(str, Enum):
    """Supported metric types"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricDefinition:
    """Definition for a custom metric"""
    name: str
    type: MetricType
    description: str
    labels: List[str] = field(default_factory=list)
    buckets: Optional[List[float]] = None
    collection_method: str = "manual"
    endpoint_url: Optional[str] = None
    json_path: Optional[str] = None
    expression: Optional[str] = None


@dataclass
class MetricsConfig:
    """Metrics module configuration"""
    enabled: bool = True
    endpoint_path: str = "/metrics"
    namespace: str = "dsp_fd2"
    default_labels: Dict[str, str] = field(default_factory=dict)
    enable_default_metrics: bool = True
    default_metrics: List[str] = field(default_factory=lambda: [
        "request_count",
        "request_latency_seconds",
        "request_size_bytes",
        "response_size_bytes",
        "active_requests",
        "error_count",
        "health_check_status",
        "upstream_latency_seconds"
    ])
    custom_metrics: List[MetricDefinition] = field(default_factory=list)
    latency_buckets: List[float] = field(default_factory=lambda: [
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
    ])
    size_buckets: List[float] = field(default_factory=lambda: [
        100, 1000, 10000, 100000, 1000000, 10000000
    ])
    collection_interval_seconds: int = 15
    module_metrics: Dict[str, List[str]] = field(default_factory=dict)
    require_auth: bool = False
    auth_module_reference: Optional[str] = None
    pushgateway_enabled: bool = False
    pushgateway_url: Optional[str] = None
    pushgateway_job: str = "dsp_fd2"
    push_interval_seconds: int = 60


class MetricsService:
    """
    Service for managing Prometheus-compatible metrics.
    
    Features:
    - Default metrics for HTTP requests (count, latency, size, errors)
    - Custom metric definitions from manifest configuration
    - Support for counter, gauge, histogram, and summary types
    - Configurable labels and histogram buckets
    - Optional Pushgateway integration
    - Thread-safe metric updates
    """
    
    def __init__(self, config: Optional[MetricsConfig] = None, use_custom_registry: bool = False):
        """
        Initialize MetricsService
        
        Args:
            config: Metrics configuration
            use_custom_registry: If True, creates a custom registry for isolation
        """
        self.config = config or MetricsConfig()
        self._registry: Optional[CollectorRegistry] = None
        self._metrics: Dict[str, Any] = {}
        self._initialized = False
        self._push_task: Optional[asyncio.Task] = None
        
        if not PROMETHEUS_AVAILABLE:
            logger.warning("prometheus_client not installed. Metrics will be disabled.")
        else:
            if use_custom_registry:
                # Create a separate registry for isolated metrics (project-specific)
                self._registry = CollectorRegistry()
                logger.debug("Created custom registry for isolated metrics")
            else:
                # Use default registry for global/shared metrics
                self._registry = REGISTRY
                logger.debug("Using default Prometheus registry")
    
    async def initialize(self):
        """Initialize metrics registry and default metrics"""
        if not PROMETHEUS_AVAILABLE:
            logger.warning("Skipping metrics initialization - prometheus_client not available")
            return
        
        if self._initialized:
            return
        
        # Initialize default metrics
        if self.config.enable_default_metrics:
            self._init_default_metrics()
        
        # Initialize custom metrics
        for metric_def in self.config.custom_metrics:
            self._create_metric(metric_def)
        
        # Start pushgateway task if enabled
        if self.config.pushgateway_enabled and self.config.pushgateway_url:
            self._push_task = asyncio.create_task(self._push_metrics_loop())
        
        self._initialized = True
        logger.info(f"MetricsService initialized with {len(self._metrics)} metrics, namespace: {self.config.namespace}")
    
    async def shutdown(self):
        """Cleanup resources"""
        if self._push_task:
            self._push_task.cancel()
            try:
                await self._push_task
            except asyncio.CancelledError:
                pass
        logger.info("MetricsService shutdown")
    
    def update_config(self, config: MetricsConfig):
        """Update metrics configuration (requires re-initialization for new metrics)"""
        self.config = config
        logger.info("MetricsService config updated")
    
    @classmethod
    def from_manifest_config(cls, manifest_config: Dict[str, Any], use_custom_registry: bool = False) -> "MetricsService":
        """Create service from manifest configuration dict"""
        custom_metrics = []
        for metric_data in manifest_config.get("custom_metrics", []):
            custom_metrics.append(MetricDefinition(
                name=metric_data.get("name", ""),
                type=MetricType(metric_data.get("type", "counter")),
                description=metric_data.get("description", ""),
                labels=metric_data.get("labels", []),
                buckets=metric_data.get("buckets"),
                collection_method=metric_data.get("collection_method", "manual"),
                endpoint_url=metric_data.get("endpoint_url"),
                json_path=metric_data.get("json_path"),
                expression=metric_data.get("expression"),
            ))
        
        config = MetricsConfig(
            enabled=manifest_config.get("enabled", True),
            endpoint_path=manifest_config.get("endpoint_path", "/metrics"),
            namespace=manifest_config.get("namespace", "dsp_fd2"),
            default_labels=manifest_config.get("default_labels", {}),
            enable_default_metrics=manifest_config.get("enable_default_metrics", True),
            default_metrics=manifest_config.get("default_metrics", [
                "request_count", "request_latency_seconds", "request_size_bytes",
                "response_size_bytes", "active_requests", "error_count",
                "health_check_status", "upstream_latency_seconds"
            ]),
            custom_metrics=custom_metrics,
            latency_buckets=manifest_config.get("latency_buckets", [
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
            ]),
            size_buckets=manifest_config.get("size_buckets", [
                100, 1000, 10000, 100000, 1000000, 10000000
            ]),
            collection_interval_seconds=manifest_config.get("collection_interval_seconds", 15),
            module_metrics=manifest_config.get("module_metrics", {}),
            require_auth=manifest_config.get("require_auth", False),
            auth_module_reference=manifest_config.get("auth_module_reference"),
            pushgateway_enabled=manifest_config.get("pushgateway_enabled", False),
            pushgateway_url=manifest_config.get("pushgateway_url"),
            pushgateway_job=manifest_config.get("pushgateway_job", "dsp_fd2"),
            push_interval_seconds=manifest_config.get("push_interval_seconds", 60),
        )
        
        return cls(config, use_custom_registry=use_custom_registry)
    
    def _init_default_metrics(self):
        """Initialize default metrics based on configuration"""
        ns = self.config.namespace
        default_labels = list(self.config.default_labels.keys())
        
        metric_definitions = {
            "request_count": {
                "type": MetricType.COUNTER,
                "name": f"{ns}_request_total",
                "description": "Total number of HTTP requests",
                "labels": default_labels + ["method", "endpoint", "status_code"],
            },
            "request_latency_seconds": {
                "type": MetricType.HISTOGRAM,
                "name": f"{ns}_request_latency_seconds",
                "description": "HTTP request latency in seconds",
                "labels": default_labels + ["method", "endpoint"],
                "buckets": self.config.latency_buckets,
            },
            "request_size_bytes": {
                "type": MetricType.HISTOGRAM,
                "name": f"{ns}_request_size_bytes",
                "description": "HTTP request size in bytes",
                "labels": default_labels + ["method", "endpoint"],
                "buckets": self.config.size_buckets,
            },
            "response_size_bytes": {
                "type": MetricType.HISTOGRAM,
                "name": f"{ns}_response_size_bytes",
                "description": "HTTP response size in bytes",
                "labels": default_labels + ["method", "endpoint"],
                "buckets": self.config.size_buckets,
            },
            "active_requests": {
                "type": MetricType.GAUGE,
                "name": f"{ns}_active_requests",
                "description": "Number of active HTTP requests",
                "labels": default_labels + ["method", "endpoint"],
            },
            "error_count": {
                "type": MetricType.COUNTER,
                "name": f"{ns}_errors_total",
                "description": "Total number of errors",
                "labels": default_labels + ["error_type", "endpoint"],
            },
            "health_check_status": {
                "type": MetricType.GAUGE,
                "name": f"{ns}_health_check_status",
                "description": "Health check status (1=healthy, 0=unhealthy)",
                "labels": default_labels + ["check_name"],
            },
            "upstream_latency_seconds": {
                "type": MetricType.HISTOGRAM,
                "name": f"{ns}_upstream_latency_seconds",
                "description": "Upstream service latency in seconds",
                "labels": default_labels + ["upstream", "method"],
                "buckets": self.config.latency_buckets,
            },
        }
        
        for metric_key in self.config.default_metrics:
            if metric_key in metric_definitions:
                defn = metric_definitions[metric_key]
                self._create_metric_from_dict(metric_key, defn)
    
    def _create_metric_from_dict(self, key: str, defn: Dict[str, Any]):
        """Create a metric from a definition dictionary"""
        if not PROMETHEUS_AVAILABLE:
            return
        
        metric_type = defn["type"]
        name = defn["name"]
        description = defn["description"]
        labels = defn.get("labels", [])
        buckets = defn.get("buckets")
        
        try:
            if metric_type == MetricType.COUNTER:
                self._metrics[key] = Counter(name, description, labels, registry=self._registry)
            elif metric_type == MetricType.GAUGE:
                self._metrics[key] = Gauge(name, description, labels, registry=self._registry)
            elif metric_type == MetricType.HISTOGRAM:
                if buckets:
                    self._metrics[key] = Histogram(name, description, labels, buckets=buckets, registry=self._registry)
                else:
                    self._metrics[key] = Histogram(name, description, labels, registry=self._registry)
            elif metric_type == MetricType.SUMMARY:
                self._metrics[key] = Summary(name, description, labels, registry=self._registry)
            
            logger.debug(f"Created metric: {name} ({metric_type.value}) in registry: {id(self._registry)}")
        except Exception as e:
            logger.warning(f"Failed to create metric {name}: {e}")
    
    def _create_metric(self, metric_def: MetricDefinition):
        """Create a metric from a MetricDefinition"""
        if not PROMETHEUS_AVAILABLE:
            return
        
        ns = self.config.namespace
        name = f"{ns}_{metric_def.name}"
        labels = list(self.config.default_labels.keys()) + metric_def.labels
        
        try:
            if metric_def.type == MetricType.COUNTER:
                self._metrics[metric_def.name] = Counter(name, metric_def.description, labels, registry=self._registry)
            elif metric_def.type == MetricType.GAUGE:
                self._metrics[metric_def.name] = Gauge(name, metric_def.description, labels, registry=self._registry)
            elif metric_def.type == MetricType.HISTOGRAM:
                buckets = metric_def.buckets or self.config.latency_buckets
                self._metrics[metric_def.name] = Histogram(name, metric_def.description, labels, buckets=buckets, registry=self._registry)
            elif metric_def.type == MetricType.SUMMARY:
                self._metrics[metric_def.name] = Summary(name, metric_def.description, labels, registry=self._registry)
            
            logger.debug(f"Created custom metric: {name} ({metric_def.type.value}) in registry: {id(self._registry)}")
        except Exception as e:
            logger.warning(f"Failed to create custom metric {name}: {e}")
    
    def _get_default_label_values(self) -> Dict[str, str]:
        """Get default label values"""
        return dict(self.config.default_labels)
    
    def inc_counter(self, metric_name: str, value: float = 1, **labels):
        """Increment a counter metric"""
        if metric_name not in self._metrics:
            logger.warning(f"Metric not found: {metric_name}")
            return
        
        metric = self._metrics[metric_name]
        all_labels = {**self._get_default_label_values(), **labels}
        
        try:
            if all_labels:
                metric.labels(**all_labels).inc(value)
            else:
                metric.inc(value)
        except Exception as e:
            logger.warning(f"Failed to increment counter {metric_name}: {e}")
    
    def set_gauge(self, metric_name: str, value: float, **labels):
        """Set a gauge metric value"""
        if metric_name not in self._metrics:
            logger.warning(f"Metric not found: {metric_name}")
            return
        
        metric = self._metrics[metric_name]
        all_labels = {**self._get_default_label_values(), **labels}
        
        try:
            if all_labels:
                metric.labels(**all_labels).set(value)
            else:
                metric.set(value)
        except Exception as e:
            logger.warning(f"Failed to set gauge {metric_name}: {e}")
    
    def inc_gauge(self, metric_name: str, value: float = 1, **labels):
        """Increment a gauge metric"""
        if metric_name not in self._metrics:
            return
        
        metric = self._metrics[metric_name]
        all_labels = {**self._get_default_label_values(), **labels}
        
        try:
            if all_labels:
                metric.labels(**all_labels).inc(value)
            else:
                metric.inc(value)
        except Exception as e:
            logger.warning(f"Failed to increment gauge {metric_name}: {e}")
    
    def dec_gauge(self, metric_name: str, value: float = 1, **labels):
        """Decrement a gauge metric"""
        if metric_name not in self._metrics:
            return
        
        metric = self._metrics[metric_name]
        all_labels = {**self._get_default_label_values(), **labels}
        
        try:
            if all_labels:
                metric.labels(**all_labels).dec(value)
            else:
                metric.dec(value)
        except Exception as e:
            logger.warning(f"Failed to decrement gauge {metric_name}: {e}")
    
    def observe_histogram(self, metric_name: str, value: float, **labels):
        """Observe a value in a histogram metric"""
        if metric_name not in self._metrics:
            logger.warning(f"Metric not found: {metric_name}")
            return
        
        metric = self._metrics[metric_name]
        all_labels = {**self._get_default_label_values(), **labels}
        
        try:
            if all_labels:
                metric.labels(**all_labels).observe(value)
            else:
                metric.observe(value)
        except Exception as e:
            logger.warning(f"Failed to observe histogram {metric_name}: {e}")
    
    def observe_summary(self, metric_name: str, value: float, **labels):
        """Observe a value in a summary metric"""
        self.observe_histogram(metric_name, value, **labels)
    
    def record_request(self, method: str, endpoint: str, status_code: int, 
                       latency_seconds: float, request_size: int = 0, response_size: int = 0):
        """Record metrics for an HTTP request"""
        self.inc_counter("request_count", method=method, endpoint=endpoint, status_code=str(status_code))
        self.observe_histogram("request_latency_seconds", latency_seconds, method=method, endpoint=endpoint)
        
        if request_size > 0:
            self.observe_histogram("request_size_bytes", request_size, method=method, endpoint=endpoint)
        if response_size > 0:
            self.observe_histogram("response_size_bytes", response_size, method=method, endpoint=endpoint)
    
    def record_error(self, error_type: str, endpoint: str):
        """Record an error"""
        self.inc_counter("error_count", error_type=error_type, endpoint=endpoint)
    
    def record_health_check(self, check_name: str, is_healthy: bool):
        """Record health check status"""
        self.set_gauge("health_check_status", 1.0 if is_healthy else 0.0, check_name=check_name)
    
    def record_upstream_latency(self, upstream: str, method: str, latency_seconds: float):
        """Record upstream service latency"""
        self.observe_histogram("upstream_latency_seconds", latency_seconds, upstream=upstream, method=method)
    
    def get_metric(self, metric_name: str) -> Optional[Any]:
        """Get a metric by name"""
        return self._metrics.get(metric_name)
    
    def get_metrics_output(self) -> bytes:
        """Generate Prometheus metrics output"""
        if not PROMETHEUS_AVAILABLE or not self._registry:
            return b"# prometheus_client not installed\n"
        
        try:
            return generate_latest(self._registry)
        except Exception as e:
            logger.error(f"Failed to generate metrics: {e}")
            return b"# Error generating metrics\n"
    
    def get_content_type(self) -> str:
        """Get the content type for metrics output"""
        if not PROMETHEUS_AVAILABLE:
            return "text/plain"
        return CONTENT_TYPE_LATEST
    
    async def _push_metrics_loop(self):
        """Background task to push metrics to Pushgateway"""
        if not PROMETHEUS_AVAILABLE:
            return
        
        while True:
            try:
                await asyncio.sleep(self.config.push_interval_seconds)
                if self.config.pushgateway_url and self._registry:
                    push_to_gateway(
                        self.config.pushgateway_url,
                        job=self.config.pushgateway_job,
                        registry=self._registry
                    )
                    logger.debug(f"Pushed metrics to {self.config.pushgateway_url}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Failed to push metrics: {e}")
    
    def get_metrics_info(self) -> Dict[str, Any]:
        """Get information about configured metrics"""
        return {
            "enabled": self.config.enabled,
            "namespace": self.config.namespace,
            "endpoint_path": self.config.endpoint_path,
            "metrics_count": len(self._metrics),
            "metrics": list(self._metrics.keys()),
            "default_labels": self.config.default_labels,
            "pushgateway_enabled": self.config.pushgateway_enabled,
            "prometheus_client_available": PROMETHEUS_AVAILABLE,
            "registry_id": id(self._registry) if self._registry else None,
        }
