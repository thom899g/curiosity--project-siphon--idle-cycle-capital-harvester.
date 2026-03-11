"""
IdleDetector Module - Multi-signal idle detection with weighted consensus
Detects true idle cycles using CPU, network, user activity, and scheduled windows.
"""

import psutil
import time
import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import platform
import os

@dataclass
class IdleSignal:
    """Individual idle detection signal with confidence"""
    name: str
    value: float
    weight: float
    threshold: float
    confidence: float = 0.0

class IdleDetector:
    """Multi-signal idle detector with weighted consensus"""
    
    def __init__(self, confidence_threshold: float = 0.8):
        self.logger = logging.getLogger("AutonomousVault.IdleDetector")
        self.confidence_threshold = confidence_threshold
        
        # Signal configuration with weights (sum to 1.0)
        self.signals: Dict[str, IdleSignal] = {
            'cpu': IdleSignal('cpu', 0.0, 0.35, 15.0),  # CPU utilization %
            'network': IdleSignal('network', 0.0, 0.25, 100.0),  # KB/s
            'user_activity': IdleSignal('user_activity', 1.0, 0.30, 0.5),  # 0=active, 1=idle
            'scheduled_window': IdleSignal('scheduled_window', 0.0, 0.10, 0.5)
        }
        
        # State tracking for trend analysis
        self.cpu_readings: List[float] = []
        self.network_readings: List[Tuple[float, float]] = []  # (sent, recv)
        self.last_network_check = time.time()
        
        # Platform-specific user activity detection
        self.system = platform.system()
        self.logger.info(f"Initialized IdleDetector for {self.system}")