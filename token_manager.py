# token_manager.py
import asyncio
import time
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import httpx
import ssl

logger = logging.getLogger("DSP-FD2.TokenManager")


class FedSSOTokenManager:
    """Manages FedSSO token generation, caching, and refresh"""
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        token_cache_ttl: int = 300,  # 5 minutes default
        request_id: Optional[str] = None
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.token_cache_ttl = token_cache_ttl
        self.request_id = request_id
        
        # Token cache
        self._cached_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._lock = asyncio.Lock()
        
        # HTTP client for token requests
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            verify=ssl.create_default_context()
        )
    
    def _resolve_vault_reference(self, secret_value: str) -> str:
        """
        Resolve Vault references in secrets.
        In a real implementation, this would integrate with your Vault manager.
        """
        # TODO: Integrate with your Vault manager
        if secret_value.startswith("vault:"):
            logger.warning("Vault references not fully implemented in TokenManager")
            # Extract the actual secret - this is a simplified version
            return secret_value.replace("vault:", "")
        return secret_value
    
    async def get_token(self, force_refresh: bool = False) -> Optional[str]:
        """
        Get a valid FedSSO token.
        Returns cached token if valid, fetches new one if expired or forced.
        """
        # Check if we have a valid cached token
        if not force_refresh and self._cached_token and self._token_expiry:
            if datetime.utcnow() < self._token_expiry:
                return self._cached_token
        
        # Acquire lock to prevent multiple concurrent token fetches
        async with self._lock:
            # Double-check after acquiring lock
            if not force_refresh and self._cached_token and self._token_expiry:
                if datetime.utcnow() < self._token_expiry:
                    return self._cached_token
            
            try:
                token = await self._fetch_new_token()
                if token:
                    # Cache the token with some buffer (e.g., 60 seconds before actual expiry)
                    self._cached_token = token
                    self._token_expiry = datetime.utcnow() + timedelta(seconds=self.token_cache_ttl)
                    logger.info(f"FedSSO token refreshed, valid until {self._token_expiry}")
                return token
            except Exception as e:
                logger.error(f"Failed to fetch FedSSO token: {e}")
                return None
    
    async def _fetch_new_token(self) -> Optional[str]:
        """Fetch a new token from FedSSO endpoint"""
        logger.info("Fetching new FedSSO token")
        
        # Resolve vault reference if needed
        client_secret = self._resolve_vault_reference(self.client_secret)
        
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": client_secret
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        if self.request_id:
            headers["X-Request-ID"] = self.request_id
        
        try:
            response = await self.http_client.post(
                self.token_url,
                data=data,
                headers=headers
            )
            response.raise_for_status()
            
            token_data = response.json()
            access_token = token_data.get("access_token")
            
            if not access_token:
                logger.error("No access_token in FedSSO response")
                return None
            
            # Extract actual expiry if available
            expires_in = token_data.get("expires_in")
            if expires_in:
                # Use actual expiry with buffer
                actual_ttl = max(60, expires_in - 60)  # Buffer of 60 seconds
                self.token_cache_ttl = actual_ttl
            
            logger.info("FedSSO token fetched successfully")
            return access_token
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching FedSSO token: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching FedSSO token: {e}")
            raise
    
    async def shutdown(self):
        """Cleanup resources"""
        await self.http_client.aclose()
        logger.info("TokenManager shutdown")
    
    def clear_cache(self):
        """Clear the cached token"""
        self._cached_token = None
        self._token_expiry = None
        logger.info("FedSSO token cache cleared")


class TokenManagerRegistry:
    """Registry for managing multiple token managers"""
    
    def __init__(self):
        self._managers: Dict[str, FedSSOTokenManager] = {}
    
    def register(
        self,
        name: str,
        client_id: str,
        client_secret: str,
        token_url: str,
        **kwargs
    ) -> FedSSOTokenManager:
        """Register a new token manager"""
        manager = FedSSOTokenManager(
            client_id=client_id,
            client_secret=client_secret,
            token_url=token_url,
            **kwargs
        )
        self._managers[name] = manager
        return manager
    
    def get(self, name: str) -> Optional[FedSSOTokenManager]:
        """Get a token manager by name"""
        return self._managers.get(name)
    
    async def get_token(self, name: str) -> Optional[str]:
        """Get token from a specific manager"""
        manager = self.get(name)
        if manager:
            return await manager.get_token()
        return None
    
    async def shutdown_all(self):
        """Shutdown all managers"""
        for manager in self._managers.values():
            await manager.shutdown()
        self._managers.clear()


# Global registry instance
token_registry = TokenManagerRegistry()
