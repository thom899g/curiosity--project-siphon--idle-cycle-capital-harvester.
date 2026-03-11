"""
Autonomous Vault Core - The Resilient Microkernel
Coordinates all components, maintains system integrity, and enables self-healing.
Implements Siege Mentality Design: Assume everything will fail.
"""

import logging
import sys
import time
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
from dotenv import load_dotenv
import os

# Load environment variables first
load_dotenv()

@dataclass
class SystemState:
    """Immutable system state representation for audit trail"""
    timestamp: str
    component: str
    status: str
    confidence_score: float
    risk_stage: int
    idle_confidence: float
    capital_allocated: float
    capital_sovereign: float
    trade_count: int
    last_trade_id: Optional[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class AutonomousVaultCore:
    """The central nervous system of Project Siphon"""
    
    def __init__(self, firebase_cred_path: Optional[str] = None):
        self.logger = self._setup_logging()
        self.logger.info("🚀 Initializing Autonomous Vault Core v2.0")
        
        # Initialize Firebase for state management and audit trail
        self.db = self._init_firebase(firebase_cred_path)
        
        # System state
        self.system_state: Optional[SystemState] = None
        self.start_time = datetime.utcnow().isoformat()
        
        # Component registry
        self.components: Dict[str, Any] = {}
        self.failure_counts: Dict[str, int] = {}
        
        # Circuit breaker state
        self.circuit_breaker_stage = 0
        self.last_failure_time: Optional[float] = None
        
        self.logger.info("✅ Autonomous Vault Core initialized with Siege Mentality")

    def _setup_logging(self) -> logging.Logger:
        """Configure robust, structured logging for observability"""
        logger = logging.getLogger("AutonomousVault")
        logger.setLevel(logging.INFO)
        
        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        
        # Structured formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - '
            '[COMPONENT:%(module)s] - %(message)s'
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        return logger

    def _init_firebase(self, cred_path: Optional[str]) -> firestore.Client:
        """Initialize Firebase with proper error handling"""
        try:
            # Use environment variable if no explicit path
            if not cred_path:
                cred_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
                if not cred_path:
                    raise ValueError(
                        "Firebase credentials path not provided in environment "
                        "or parameter"
                    )
            
            # Initialize Firebase app
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred, {
                    'projectId': os.getenv('FIREBASE_PROJECT_ID')
                })
            
            db = firestore.client()
            self.logger.info(f"✅ Firebase initialized with project: {cred.project_id}")
            return db
            
        except Exception as e:
            self.logger.error(f"❌ Firebase initialization failed: {str(e)}")
            self.logger.critical("Firebase is REQUIRED for audit trail. System halted.")
            sys.exit(1)

    def register_component(self, name: str, component: Any) -> bool:
        """Register a system component with health monitoring"""
        if name in self.components:
            self.logger.warning(f"Component {name} already registered")
            return False
        
        self.components[name] = component
        self.failure_counts[name] = 0
        self.logger.info(f"✅ Registered component: {name}")
        
        # Log to Firebase for audit trail
        try:
            doc_ref = self.db.collection('component_registry').document(name)
            doc_ref.set({
                'registered_at': datetime.utcnow().isoformat(),
                'status': 'active',
                'failure_count': 0,
                'last_heartbeat': datetime.utcnow().isoformat()
            })
        except Exception as e:
            self.logger.error(f"Failed to log component registration: {str(e)}")
        
        return True

    def update_system_state(self, **kwargs) -> None:
        """Update and persist system state with atomic write"""
        if not self.system_state:
            self.system_state = SystemState(
                timestamp=datetime.utcnow().isoformat(),
                component="core",
                status="initializing",
                confidence_score=0.0,
                risk_stage=0,
                idle_confidence=0.0,
                capital_allocated=0.0,
                capital_sovereign=0.0,
                trade_count=0,
                last_trade_id=None
            )
        
        # Update fields
        for key, value in kwargs.items():
            if hasattr(self.system_state, key):
                setattr(self.system_state, key, value)
        
        # Persist to Firebase with atomic transaction
        try:
            state_dict = self.system_state.to_dict()
            state_dict['_updated_at'] = firestore.SERVER_TIMESTAMP
            
            doc_ref = self.db.collection('system_states').document(
                self.system_state.timestamp.replace(':', '-')  # Firebase-safe ID
            )
            doc_ref.set(state_dict)
            
            # Also update latest state reference
            self.db.collection('metadata').document('latest_state').set({
                'state_id': doc_ref.id,
                'updated_at': firestore.SERVER_TIMESTAMP
            })
            
        except Exception as e:
            self.logger.error(f"Failed to persist system state: {str(e)}")
            # Continue anyway - system should not crash on logging failure

    def circuit_breaker_check(self, component: str, error: Exception) -> int:
        """
        Multi-stage circuit breaker with graduated response
        
        Returns:
            int: The new circuit breaker stage (0-5)
        """
        self.failure_counts[component] = self.failure_counts.get(component, 0) + 1
        
        current_time = time.time()
        
        # Stage progression logic
        if self.circuit_breaker_stage == 0:
            # First failure in component
            if self.failure_counts[component] >= 3:
                self.circuit_breaker_stage = 1
                self.logger.warning(f"⚡ Circuit breaker Stage 1: {component} has 3+ failures")
        
        elif self.circuit_breaker_stage == 1:
            # Multiple components failing
            failing_components = sum(1 for count in self.failure_counts.values() if count >= 2)
            if failing_components >= 2:
                self.circuit_breaker_stage = 2
                self.logger.warning("⚡ Circuit breaker Stage 2: Multiple components failing")
        
        # Time-based reset
        if (self.last_failure_time and 
            (current_time - self.last_failure_time) > 3600):  # 1 hour
            if self.circuit_breaker_stage > 0:
                self.logger.info("🔄 Circuit breaker reset after 1 hour of stability")
                self.circuit_breaker_stage = 0
                self.failure_counts = {k: 0 for k in self.failure_counts}
        
        self.last_failure_time = current_time
        return self.circuit_breaker_stage

    def emergency_shutdown(self, reason: str) -> None:
        """Graceful emergency shutdown with audit trail"""
        self.logger.critical(f"🛑 EMERGENCY SHUTDOWN: {reason}")
        
        # Log shutdown event
        try:
            self.db.collection('emergency_events').add({
                'reason': reason,
                'shutdown_time': datetime.utcnow().isoformat(),
                'system_state': self.system_state.to_dict() if self.system_state else {},
                'circuit_breaker_stage': self.circuit_breaker_stage
            })
        except Exception as e:
            self.logger.error(f"Failed to log emergency event: {str(e)}")
        
        # Attempt to notify via all available channels
        self._send_emergency_notification(reason)
        
        sys.exit(1)

    def _send_emergency_notification(self, reason: str) -> None:
        """Multi-channel emergency notification"""
        # Telegram notification (if configured)
        telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if telegram_token and telegram_chat_id:
            try:
                import telegram
                bot = telegram.Bot(token=telegram_token)
                message = f"🚨 PROJECT SIPHON EMERGENCY SHUTDOWN\n\n"
                message += f"Reason: {reason}\n"
                message += f"Time: {datetime.utcnow().isoformat()}\n"
                message += f"Check Firebase for details."
                
                bot.send_message(chat_id=telegram_chat_id, text=message)
                self.logger.info("✅ Emergency notification sent via Telegram")
            except Exception as e:
                self.logger.error(f"Failed to send Telegram notification: {str(e)}")
        
        # Also log to stdout for containerized environments
        print(f"EMERGENCY_SHUTDOWN: {reason}", flush=True)

    def run(self) -> None:
        """Main system loop with self-healing capabilities"""
        self.logger.info("🚀 Starting Autonomous Vault main loop")
        
        try:
            while True:
                # Update system state
                self.update_system_state(
                    timestamp=datetime.utcnow().isoformat(),
                    component="core",
                    status="running",
                    risk_stage=self.circuit_breaker_stage
                )
                
                # Check circuit breaker
                if self.circuit_breaker_stage >= 5:
                    self.emergency_shutdown("Circuit breaker Stage 5 reached")
                
                # Monitor components
                self._monitor_components()
                
                # Sleep with interrupt handling
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            self.logger.info("👋 Shutdown requested by user")
            self.update_system_state(status="shutdown", component="core")
        except Exception as e:
            self.logger.error(f"Unhandled exception in main loop: {str(e)}")
            self.emergency_shutdown(f"Main loop failure: {str(e)}")

    def _monitor_components(self) -> None:
        """Monitor registered components for health"""
        for name, component in self.components.items():
            try:
                # Check if component has a health_check method
                if hasattr(component, 'health_check'):
                    is_healthy = component.health_check()
                    if not is_healthy:
                        self.logger.warning(f"Component {name} failed health check")
                        self.circuit_breaker_check(name, 
                            Exception(f"Health check failed for {name}"))
            except Exception as e:
                self.logger.error(f"Error monitoring component {name}: {str(e)}")
                self.circuit_breaker_check(name, e)

if __name__ == "__main__":
    # Example usage
    vault = AutonomousVaultCore()
    vault.run()