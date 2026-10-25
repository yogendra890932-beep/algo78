# backend/engine/market_structure.py
"""
Market Structure Engine for 1-Minute Index Option Premium Scalping (NIFTY, BANKNIFTY, SENSEX)
Analyzes pure OHLCV price action to detect Swings, Trend, Breaks, Pullbacks, Recoveries,
Liquidity Sweeps, Price Patterns, Market Phases, and structural confidence.
NO INDICATORS (EMA, VWAP, ATR, etc.) and NO TRADE RECOMMENDATIONS.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple
import numpy as np
import pandas as pd


# ==============================================================================
# ENUMS
# ==============================================================================

class TrendDirection(Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    RANGING = "RANGING"
    TRANSITIONAL = "TRANSITIONAL"


class MarketPhase(Enum):
    TRENDING = "TRENDING"
    PULLBACK = "PULLBACK"
    RECOVERY = "RECOVERY"
    BREAKOUT = "BREAKOUT"
    COMPRESSION = "COMPRESSION"
    EXPANSION = "EXPANSION"
    ACCUMULATION = "ACCUMULATION"
    DISTRIBUTION = "DISTRIBUTION"
    TRANSITION = "TRANSITION"
    RANGE = "RANGE"


class SwingType(Enum):
    HIGH = "HIGH"
    LOW = "LOW"


class SwingLevel(Enum):
    MAJOR = "MAJOR"
    MINOR = "MINOR"
    INTERNAL = "INTERNAL"
    EXTERNAL = "EXTERNAL"


class BreakType(Enum):
    BOS = "BOS"
    CHOCH = "CHOCH"


class PullbackType(Enum):
    HEALTHY = "HEALTHY"
    DEEP = "DEEP"
    FAILED = "FAILED"
    NESTED = "NESTED"
    NONE = "NONE"


class RecoveryStatus(Enum):
    ACTIVE = "ACTIVE"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    NONE = "NONE"


class LiquidityType(Enum):
    EQH = "EQH"
    EQL = "EQL"
    SWEEP_HIGH = "SWEEP_HIGH"
    SWEEP_LOW = "SWEEP_LOW"
    STOP_HUNT = "STOP_HUNT"
    SFP = "SFP"
    FALSE_BREAKOUT = "FALSE_BREAKOUT"
    BREAKOUT_TRAP = "BREAKOUT_TRAP"


class PatternType(Enum):
    DOUBLE_TOP = "DOUBLE_TOP"
    DOUBLE_BOTTOM = "DOUBLE_BOTTOM"
    TRIPLE_TOP = "TRIPLE_TOP"
    TRIPLE_BOTTOM = "TRIPLE_BOTTOM"
    BULL_FLAG = "BULL_FLAG"
    BEAR_FLAG = "BEAR_FLAG"
    ASCENDING_TRIANGLE = "ASCENDING_TRIANGLE"
    DESCENDING_TRIANGLE = "DESCENDING_TRIANGLE"
    SYMMETRICAL_TRIANGLE = "SYMMETRICAL_TRIANGLE"
    RISING_WEDGE = "RISING_WEDGE"
    FALLING_WEDGE = "FALLING_WEDGE"
    RISING_CHANNEL = "RISING_CHANNEL"
    FALLING_CHANNEL = "FALLING_CHANNEL"
    RECTANGLE = "RECTANGLE"
    HEAD_AND_SHOULDERS = "HEAD_AND_SHOULDERS"
    INVERSE_HEAD_AND_SHOULDERS = "INVERSE_HEAD_AND_SHOULDERS"


class PatternStatus(Enum):
    FORMING = "FORMING"
    CONFIRMED = "CONFIRMED"
    BROKEN = "BROKEN"


# ==============================================================================
# DATACLASSES (Slots Enabled, Frozen/Immutable)
# ==============================================================================

@dataclass(slots=True, frozen=True)
class SwingPoint:
    index: int
    price: float
    type: SwingType
    level: SwingLevel
    strength: float           # 0 to 100
    quality: float            # 0 to 100
    age: int                  # Bars since swing
    momentum: float           # Preceding momentum slope
    volume: float             # Volume at swing candle


@dataclass(slots=True, frozen=True)
class TrendMetrics:
    direction: TrendDirection
    strength: float           # 0 to 100
    quality: float            # 0 to 100
    age: int                  # Bars since trend change
    momentum: float           # Average slope of impulse legs
    exhaustion: float         # 0 to 100
    continuation_probability: float  # 0 to 100


@dataclass(slots=True, frozen=True)
class StructureBreak:
    index: int
    type: BreakType
    direction: TrendDirection
    level: float
    confirmed: bool


@dataclass(slots=True, frozen=True)
class PriceLevel:
    price: float
    strength: float           # 0 to 100
    touches: int
    age: int                  # Bars since level creation
    is_protected: bool


@dataclass(slots=True, frozen=True)
class SwingZone:
    level_start: float
    level_end: float
    type: str                 # "SUPPLY" or "DEMAND"
    mitigated: bool
    mitigation_index: Optional[int] = None


@dataclass(slots=True, frozen=True)
class PullbackMetrics:
    type: PullbackType
    quality: float            # 0 to 100
    strength: float           # 0 to 100
    duration: int             # Bars in pullback
    depth_pct: float          # Retracement percentage (0 to 100+)


@dataclass(slots=True, frozen=True)
class RecoveryMetrics:
    status: RecoveryStatus
    quality: float            # 0 to 100
    strength: float           # 0 to 100
    confidence: float         # 0 to 100


@dataclass(slots=True, frozen=True)
class LiquidityEvent:
    index: int
    type: LiquidityType
    price: float
    volume: float
    description: str


@dataclass(slots=True, frozen=True)
class PricePattern:
    type: PatternType
    direction: TrendDirection
    confidence: float         # 0 to 100
    status: PatternStatus
    breakout_level: float
    invalidation_level: float
    age: int                  # Bars since pattern detection


@dataclass(slots=True, frozen=True)
class StructureResult:
    swing_highs: List[SwingPoint]
    swing_lows: List[SwingPoint]
    trend: TrendMetrics
    recent_breaks: List[StructureBreak]
    protected_high: Optional[PriceLevel]
    protected_low: Optional[PriceLevel]
    support_levels: List[PriceLevel]
    resistance_levels: List[PriceLevel]
    swing_zones: List[SwingZone]
    pullback: PullbackMetrics
    recovery: RecoveryMetrics
    liquidity_events: List[LiquidityEvent]
    patterns: List[PricePattern]
    phase: MarketPhase
    confidence_score: float   # 0 to 100


# ==============================================================================
# CORE STATE MACHINE & ENGINE (OPTIMIZED)
# ==============================================================================

class MarketStructureEngine:
    def __init__(self, max_history: int = 1000):
        self.max_history = max_history
        
        # State Arrays/Queues
        self.candles: List[dict] = []
        self.global_index = -1
        
        # Keep lists truncated for constant-memory and O(1) trailing checks
        self.swing_highs: List[SwingPoint] = []
        self.swing_lows: List[SwingPoint] = []
        self.recent_breaks: List[StructureBreak] = []
        self.swing_zones: List[SwingZone] = []
        self.liquidity_events: List[LiquidityEvent] = []
        self.patterns: List[PricePattern] = []
        
        self.support_levels: List[PriceLevel] = []
        self.resistance_levels: List[PriceLevel] = []
        
        # Trend State Machine
        self.trend_direction = TrendDirection.RANGING
        self.trend_age = 0
        self.consecutive_bos = 0
        self.protected_high: Optional[PriceLevel] = None
        self.protected_low: Optional[PriceLevel] = None
        
        # Pullback State Machine
        self.pullback_active = False
        self.pullback_start_idx = -1
        self.pullback_max_depth = 0.0
        self.pullback_type = PullbackType.NONE
        self.pullback_duration = 0
        
        # Recovery State Machine
        self.recovery_status = RecoveryStatus.NONE
        self.recovery_start_idx = -1
        self.recovery_highest_price = 0.0
        self.recovery_lowest_price = 0.0
        
        # Windows config
        self.major_w = 5
        self.minor_w = 2

    def update(self, candle: dict, full_analysis: bool = True) -> Optional[StructureResult]:
        """
        Processes a single candle incrementally and returns the latest market structure if full_analysis is True.
        """
        self.global_index += 1
        self.candles.append(candle)
        
        # Maintain max history of raw candles
        if len(self.candles) > self.max_history:
            self.candles.pop(0)
            
        # 1. Update Trend Age
        if self.trend_direction != TrendDirection.RANGING:
            self.trend_age += 1
            
        # 2. Detect Swings (confirmed at minor_w and major_w delay)
        self._detect_swings()
        
        # 3. Detect Structure Breaks (BOS / CHOCH) and update Protected High/Low
        self._detect_structure_breaks()
        
        # 4. Mitigate Swing Zones (unconditional state update, fast)
        self._update_zones_mitigation()
        
        # 5. Analyze Pullbacks & Recoveries
        self._analyze_pullback_recovery()
        
        # 6. Detect Liquidity Sweeps, SFPs, and False Breakouts
        self._detect_liquidity()
        
        if not full_analysis:
            return None
            
        # 7. Recompute Support/Resistance levels (expensive O(N*K) touch check)
        self._update_sr_levels()
        
        # 8. Price Pattern Recognition
        self._detect_price_patterns()
        
        # 9. Determine Market Phase
        phase = self._determine_market_phase()
        
        # 10. Compute Confidence Score
        confidence = self._compute_confidence_score(phase)
        
        # Build and return StructureResult
        return StructureResult(
            swing_highs=list(self.swing_highs),
            swing_lows=list(self.swing_lows),
            trend=self._get_trend_metrics(),
            recent_breaks=list(self.recent_breaks[-10:]),
            protected_high=self.protected_high,
            protected_low=self.protected_low,
            support_levels=list(self.support_levels[:10]),
            resistance_levels=list(self.resistance_levels[:10]),
            swing_zones=list(self.swing_zones),
            pullback=self._get_pullback_metrics(),
            recovery=self._get_recovery_metrics(),
            liquidity_events=list(self.liquidity_events[-10:]),
            patterns=list(self.patterns[-5:]),
            phase=phase,
            confidence_score=confidence
        )

    # --------------------------------------------------------------------------
    # HELPERS & PROCESSING FUNCTIONS (SPEED OPTIMIZED)
    # --------------------------------------------------------------------------

    def _get_candle_by_abs_idx(self, abs_idx: int) -> Optional[dict]:
        offset = abs_idx - (self.global_index - len(self.candles) + 1)
        if 0 <= offset < len(self.candles):
            return self.candles[offset]
        return None

    def _is_swing_high_rel(self, rel_idx: int, window: int) -> bool:
        # rel_idx is relative to the end of self.candles, e.g. -3 for minor swing
        idx_cand = len(self.candles) + rel_idx
        if not (0 <= idx_cand < len(self.candles)):
            return False
        val = self.candles[idx_cand]["high"]
        for offset in range(-window, window + 1):
            if offset == 0:
                continue
            idx = idx_cand + offset
            if not (0 <= idx < len(self.candles)):
                return False
            if self.candles[idx]["high"] > val:
                return False
            if self.candles[idx]["high"] == val and offset > 0:
                return False
        return True

    def _is_swing_low_rel(self, rel_idx: int, window: int) -> bool:
        idx_cand = len(self.candles) + rel_idx
        if not (0 <= idx_cand < len(self.candles)):
            return False
        val = self.candles[idx_cand]["low"]
        for offset in range(-window, window + 1):
            if offset == 0:
                continue
            idx = idx_cand + offset
            if not (0 <= idx < len(self.candles)):
                return False
            if self.candles[idx]["low"] < val:
                return False
            if self.candles[idx]["low"] == val and offset > 0:
                return False
        return True

    def _calculate_swing_metrics(self, abs_idx: int, stype: SwingType) -> Tuple[float, float, float]:
        """Returns (strength, quality, momentum) for a swing candle."""
        cand = self._get_candle_by_abs_idx(abs_idx)
        if cand is None:
            return 0.0, 0.0, 0.0
        
        # Wick Rejection Quality
        rng = cand["high"] - cand["low"]
        wick_ratio = 0.0
        if rng > 0:
            if stype == SwingType.HIGH:
                wick_ratio = (cand["high"] - max(cand["open"], cand["close"])) / rng
            else:
                wick_ratio = (min(cand["open"], cand["close"]) - cand["low"]) / rng
        quality = min(100.0, wick_ratio * 150.0)
        
        # Volume Expansion Quality
        vol = cand["volume"]
        avg_vol = 1.0
        buffer_offset = abs_idx - (self.global_index - len(self.candles) + 1)
        if buffer_offset > 0:
            slice_start = max(0, buffer_offset - 10)
            prev_vols = [c["volume"] for c in self.candles[slice_start:buffer_offset]]
            if prev_vols:
                avg_vol = sum(prev_vols) / len(prev_vols)
        vol_ratio = vol / max(1.0, avg_vol)
        quality = min(100.0, (quality + min(50.0, (vol_ratio - 1.0) * 25.0)))
        
        # Preceding Momentum Slope
        prev_cand = self._get_candle_by_abs_idx(abs_idx - 3)
        momentum = 0.0
        if prev_cand:
            momentum = (cand["close"] - prev_cand["close"]) / 3.0
            
        # Strength: measured by price movement after swing
        curr_price = self.candles[-1]["close"]
        if stype == SwingType.HIGH:
            price_diff = cand["high"] - curr_price
        else:
            price_diff = curr_price - cand["low"]
        strength = min(100.0, max(0.0, (price_diff / max(0.1, cand["close"])) * 500.0))
        
        return strength, quality, momentum

    def _add_swing_high(self, sw: SwingPoint):
        self.swing_highs.append(sw)
        if len(self.swing_highs) > 30:
            self.swing_highs.pop(0)

    def _add_swing_low(self, sw: SwingPoint):
        self.swing_lows.append(sw)
        if len(self.swing_lows) > 30:
            self.swing_lows.pop(0)

    def _detect_swings(self):
        # 1. Detect Minor Swings at delay = minor_w
        minor_idx = self.global_index - self.minor_w
        if minor_idx >= self.minor_w:
            # Check High (rel_idx = -1 - minor_w = -3)
            if self._is_swing_high_rel(-3, self.minor_w):
                level = SwingLevel.MINOR
                if self.swing_highs and self.swing_lows:
                    last_maj_h = self._get_last_major_swing(SwingType.HIGH)
                    last_maj_l = self._get_last_major_swing(SwingType.LOW)
                    if last_maj_h and last_maj_l and last_maj_l.price < self.candles[-1]["high"] < last_maj_h.price:
                        level = SwingLevel.INTERNAL
                        
                cand = self._get_candle_by_abs_idx(minor_idx)
                if cand and not any(s.index == minor_idx for s in self.swing_highs):
                    strg, qual, mom = self._calculate_swing_metrics(minor_idx, SwingType.HIGH)
                    sw = SwingPoint(
                        index=minor_idx, price=cand["high"], type=SwingType.HIGH, level=level,
                        strength=strg, quality=qual, age=self.minor_w, momentum=mom, volume=cand["volume"]
                    )
                    self._add_swing_high(sw)
                    self.swing_zones.append(SwingZone(
                        level_start=min(cand["open"], cand["close"]), level_end=cand["high"],
                        type="SUPPLY", mitigated=False
                    ))
                    if len(self.swing_zones) > 30:
                        self.swing_zones.pop(0)
                    
            # Check Low
            if self._is_swing_low_rel(-3, self.minor_w):
                level = SwingLevel.MINOR
                if self.swing_highs and self.swing_lows:
                    last_maj_h = self._get_last_major_swing(SwingType.HIGH)
                    last_maj_l = self._get_last_major_swing(SwingType.LOW)
                    if last_maj_h and last_maj_l and last_maj_l.price < self.candles[-1]["low"] < last_maj_h.price:
                        level = SwingLevel.INTERNAL
                        
                cand = self._get_candle_by_abs_idx(minor_idx)
                if cand and not any(s.index == minor_idx for s in self.swing_lows):
                    strg, qual, mom = self._calculate_swing_metrics(minor_idx, SwingType.LOW)
                    sw = SwingPoint(
                        index=minor_idx, price=cand["low"], type=SwingType.LOW, level=level,
                        strength=strg, quality=qual, age=self.minor_w, momentum=mom, volume=cand["volume"]
                    )
                    self._add_swing_low(sw)
                    self.swing_zones.append(SwingZone(
                        level_start=cand["low"], level_end=max(cand["open"], cand["close"]),
                        type="DEMAND", mitigated=False
                    ))
                    if len(self.swing_zones) > 30:
                        self.swing_zones.pop(0)

        # 2. Detect Major Swings at delay = major_w (and upgrade minor swings)
        major_idx = self.global_index - self.major_w
        if major_idx >= self.major_w:
            # Upgrade High (rel_idx = -1 - major_w = -6)
            if self._is_swing_high_rel(-6, self.major_w):
                found = False
                for idx, sw in enumerate(self.swing_highs):
                    if sw.index == major_idx:
                        self.swing_highs[idx] = SwingPoint(
                            index=sw.index, price=sw.price, type=sw.type, level=SwingLevel.MAJOR,
                            strength=sw.strength, quality=sw.quality, age=sw.age, momentum=sw.momentum, volume=sw.volume
                        )
                        found = True
                        break
                if not found:
                    cand = self._get_candle_by_abs_idx(major_idx)
                    if cand:
                        strg, qual, mom = self._calculate_swing_metrics(major_idx, SwingType.HIGH)
                        self._add_swing_high(SwingPoint(
                            index=major_idx, price=cand["high"], type=SwingType.HIGH, level=SwingLevel.MAJOR,
                            strength=strg, quality=qual, age=self.major_w, momentum=mom, volume=cand["volume"]
                        ))
            
            # Upgrade Low
            if self._is_swing_low_rel(-6, self.major_w):
                found = False
                for idx, sw in enumerate(self.swing_lows):
                    if sw.index == major_idx:
                        self.swing_lows[idx] = SwingPoint(
                            index=sw.index, price=sw.price, type=sw.type, level=SwingLevel.MAJOR,
                            strength=sw.strength, quality=sw.quality, age=sw.age, momentum=sw.momentum, volume=sw.volume
                        )
                        found = True
                        break
                if not found:
                    cand = self._get_candle_by_abs_idx(major_idx)
                    if cand:
                        strg, qual, mom = self._calculate_swing_metrics(major_idx, SwingType.LOW)
                        self._add_swing_low(SwingPoint(
                            index=major_idx, price=cand["low"], type=SwingType.LOW, level=SwingLevel.MAJOR,
                            strength=strg, quality=sw.quality, age=sw.age, momentum=mom, volume=cand["volume"]
                        ))

    def _get_last_major_swing(self, stype: SwingType) -> Optional[SwingPoint]:
        arr = self.swing_highs if stype == SwingType.HIGH else self.swing_lows
        majors = [s for s in arr if s.level == SwingLevel.MAJOR]
        return majors[-1] if majors else None

    # --------------------------------------------------------------------------
    # STRUCTURE BREAKS (BOS & CHOCH)
    # --------------------------------------------------------------------------

    def _detect_structure_breaks(self):
        if not self.candles:
            return
        
        curr_close = self.candles[-1]["close"]
        last_maj_h = self._get_last_major_swing(SwingType.HIGH)
        last_maj_l = self._get_last_major_swing(SwingType.LOW)
        
        if not last_maj_h or not last_maj_l:
            self.trend_direction = TrendDirection.RANGING
            return
            
        if self.trend_direction == TrendDirection.RANGING:
            if curr_close > last_maj_h.price:
                self.trend_direction = TrendDirection.BULLISH
                self.trend_age = 0
                self.consecutive_bos = 1
                self.recent_breaks.append(StructureBreak(
                    index=self.global_index, type=BreakType.BOS, direction=TrendDirection.BULLISH,
                    level=last_maj_h.price, confirmed=True
                ))
                if len(self.recent_breaks) > 20:
                    self.recent_breaks.pop(0)
                self._update_protected_low(last_maj_h.index)
            elif curr_close < last_maj_l.price:
                self.trend_direction = TrendDirection.BEARISH
                self.trend_age = 0
                self.consecutive_bos = 1
                self.recent_breaks.append(StructureBreak(
                    index=self.global_index, type=BreakType.BOS, direction=TrendDirection.BEARISH,
                    level=last_maj_l.price, confirmed=True
                ))
                if len(self.recent_breaks) > 20:
                    self.recent_breaks.pop(0)
                self._update_protected_high(last_maj_l.index)
                
        elif self.trend_direction == TrendDirection.BULLISH:
            if curr_close > last_maj_h.price:
                self.consecutive_bos += 1
                self.recent_breaks.append(StructureBreak(
                    index=self.global_index, type=BreakType.BOS, direction=TrendDirection.BULLISH,
                    level=last_maj_h.price, confirmed=True
                ))
                if len(self.recent_breaks) > 20:
                    self.recent_breaks.pop(0)
                self._update_protected_low(last_maj_h.index)
                
            limit = self.protected_low.price if self.protected_low else last_maj_l.price
            if curr_close < limit:
                self.trend_direction = TrendDirection.BEARISH
                self.trend_age = 0
                self.consecutive_bos = 0
                self.recent_breaks.append(StructureBreak(
                    index=self.global_index, type=BreakType.CHOCH, direction=TrendDirection.BEARISH,
                    level=limit, confirmed=True
                ))
                if len(self.recent_breaks) > 20:
                    self.recent_breaks.pop(0)
                self._update_protected_high(self.global_index)
                
        elif self.trend_direction == TrendDirection.BEARISH:
            if curr_close < last_maj_l.price:
                self.consecutive_bos += 1
                self.recent_breaks.append(StructureBreak(
                    index=self.global_index, type=BreakType.BOS, direction=TrendDirection.BEARISH,
                    level=last_maj_l.price, confirmed=True
                ))
                if len(self.recent_breaks) > 20:
                    self.recent_breaks.pop(0)
                self._update_protected_high(last_maj_l.index)
                
            limit = self.protected_high.price if self.protected_high else last_maj_h.price
            if curr_close > limit:
                self.trend_direction = TrendDirection.BULLISH
                self.trend_age = 0
                self.consecutive_bos = 0
                self.recent_breaks.append(StructureBreak(
                    index=self.global_index, type=BreakType.CHOCH, direction=TrendDirection.BULLISH,
                    level=limit, confirmed=True
                ))
                if len(self.recent_breaks) > 20:
                    self.recent_breaks.pop(0)
                self._update_protected_low(self.global_index)

    def _update_protected_low(self, break_origin_idx: int):
        if not self.swing_lows:
            return
        lowest_low = None
        for sw in reversed(self.swing_lows):
            if sw.index <= break_origin_idx:
                if lowest_low is None or sw.price < lowest_low.price:
                    lowest_low = sw
        if lowest_low:
            self.protected_low = PriceLevel(
                price=lowest_low.price, strength=100.0, touches=1,
                age=self.global_index - lowest_low.index, is_protected=True
            )

    def _update_protected_high(self, break_origin_idx: int):
        if not self.swing_highs:
            return
        highest_high = None
        for sw in reversed(self.swing_highs):
            if sw.index <= break_origin_idx:
                if highest_high is None or sw.price > highest_high.price:
                    highest_high = sw
        if highest_high:
            self.protected_high = PriceLevel(
                price=highest_high.price, strength=100.0, touches=1,
                age=self.global_index - highest_high.index, is_protected=True
            )

    # --------------------------------------------------------------------------
    # ZONES AND SUPPORT / RESISTANCE
    # --------------------------------------------------------------------------

    def _update_zones_mitigation(self):
        if not self.candles:
            return
        curr_close = self.candles[-1]["close"]
        for idx in range(len(self.swing_zones)):
            zone = self.swing_zones[idx]
            if not zone.mitigated:
                if zone.type == "SUPPLY" and curr_close > zone.level_end:
                    self.swing_zones[idx] = SwingZone(
                        level_start=zone.level_start, level_end=zone.level_end,
                        type=zone.type, mitigated=True, mitigation_index=self.global_index
                    )
                elif zone.type == "DEMAND" and curr_close < zone.level_start:
                    self.swing_zones[idx] = SwingZone(
                        level_start=zone.level_start, level_end=zone.level_end,
                        type=zone.type, mitigated=True, mitigation_index=self.global_index
                    )

    def _update_sr_levels(self):
        curr_close = self.candles[-1]["close"]
        candle_slice = self.candles[-100:]
        
        # Support levels
        supports: List[PriceLevel] = []
        if self.protected_low:
            supports.append(self.protected_low)
            
        for sw in reversed(self.swing_lows[-15:]):
            if curr_close > sw.price:
                touches = 0
                sw_price = sw.price
                for c in candle_slice:
                    if abs(c["low"] - sw_price) / sw_price < 0.0015:
                        touches += 1
                strength = min(100.0, 50.0 + (touches * 10) - ((self.global_index - sw.index) * 0.05))
                supports.append(PriceLevel(
                    price=sw_price, strength=max(10.0, strength), touches=touches,
                    age=self.global_index - sw.index, is_protected=False
                ))
        supports = sorted(supports, key=lambda x: x.price, reverse=True)
        filtered_supports: List[PriceLevel] = []
        for s in supports:
            if not any(abs(s.price - fs.price) / s.price < 0.002 for fs in filtered_supports):
                filtered_supports.append(s)
        self.support_levels = filtered_supports

        # Resistance levels
        resistances: List[PriceLevel] = []
        if self.protected_high:
            resistances.append(self.protected_high)
            
        for sw in reversed(self.swing_highs[-15:]):
            if curr_close < sw.price:
                touches = 0
                sw_price = sw.price
                for c in candle_slice:
                    if abs(c["high"] - sw_price) / sw_price < 0.0015:
                        touches += 1
                strength = min(100.0, 50.0 + (touches * 10) - ((self.global_index - sw.index) * 0.05))
                resistances.append(PriceLevel(
                    price=sw_price, strength=max(10.0, strength), touches=touches,
                    age=self.global_index - sw.index, is_protected=False
                ))
        resistances = sorted(resistances, key=lambda x: x.price)
        filtered_resistances: List[PriceLevel] = []
        for r in resistances:
            if not any(abs(r.price - fr.price) / r.price < 0.002 for fr in filtered_resistances):
                filtered_resistances.append(r)
        self.resistance_levels = filtered_resistances

    # --------------------------------------------------------------------------
    # PULLBACK & RECOVERY ANALYSIS
    # --------------------------------------------------------------------------

    def _analyze_pullback_recovery(self):
        if not self.candles or len(self.candles) < 5:
            return
            
        curr_close = self.candles[-1]["close"]
        last_maj_h = self._get_last_major_swing(SwingType.HIGH)
        last_maj_l = self._get_last_major_swing(SwingType.LOW)
        
        if not last_maj_h or not last_maj_l:
            return
            
        # 1. Pullback Detection
        if self.trend_direction == TrendDirection.BULLISH:
            offset = max(0, len(self.candles) - (self.global_index - last_maj_l.index + 1))
            highest_high = max(c["high"] for c in self.candles[offset:])
            impulse_height = highest_high - last_maj_l.price
            
            if impulse_height > 0:
                depth = highest_high - curr_close
                depth_pct = (depth / impulse_height) * 100.0
                
                if depth_pct >= 15.0:
                    if not self.pullback_active:
                        self.pullback_active = True
                        self.pullback_start_idx = self.global_index
                        self.pullback_duration = 0
                    self.pullback_duration += 1
                    self.pullback_max_depth = max(self.pullback_max_depth, depth_pct)
                    
                    if depth_pct < 50.0:
                        self.pullback_type = PullbackType.HEALTHY
                    elif depth_pct < 100.0:
                        self.pullback_type = PullbackType.DEEP
                    else:
                        self.pullback_type = PullbackType.FAILED
                else:
                    # Price is back near the impulse highs. If a recovery
                    # is underway/confirmed, keep the pullback context for
                    # this bar so the recovery state machine can confirm
                    # (and downstream strategies can observe
                    # pullback+CONFIRMED together) instead of wiping the
                    # pullback state one bar early.
                    if self.pullback_active and self.pullback_type in (
                            PullbackType.HEALTHY, PullbackType.DEEP,
                            PullbackType.NESTED) and (
                            self.recovery_status == RecoveryStatus.ACTIVE):
                        self.pullback_duration += 1
                    else:
                        self.pullback_active = False
                        self.pullback_type = PullbackType.NONE
                        self.pullback_max_depth = 0.0
                        self.pullback_duration = 0
                    
        elif self.trend_direction == TrendDirection.BEARISH:
            offset = max(0, len(self.candles) - (self.global_index - last_maj_h.index + 1))
            lowest_low = min(c["low"] for c in self.candles[offset:])
            impulse_height = last_maj_h.price - lowest_low
            
            if impulse_height > 0:
                depth = curr_close - lowest_low
                depth_pct = (depth / impulse_height) * 100.0
                
                if depth_pct >= 15.0:
                    if not self.pullback_active:
                        self.pullback_active = True
                        self.pullback_start_idx = self.global_index
                        self.pullback_duration = 0
                    self.pullback_duration += 1
                    self.pullback_max_depth = max(self.pullback_max_depth, depth_pct)
                    
                    if depth_pct < 50.0:
                        self.pullback_type = PullbackType.HEALTHY
                    elif depth_pct < 100.0:
                        self.pullback_type = PullbackType.DEEP
                    else:
                        self.pullback_type = PullbackType.FAILED
                else:
                    # Same as the bullish branch: keep the pullback
                    # context while a recovery is active/confirmed.
                    if self.pullback_active and self.pullback_type in (
                            PullbackType.HEALTHY, PullbackType.DEEP,
                            PullbackType.NESTED) and (
                            self.recovery_status == RecoveryStatus.ACTIVE):
                        self.pullback_duration += 1
                    else:
                        self.pullback_active = False
                        self.pullback_type = PullbackType.NONE
                        self.pullback_max_depth = 0.0
                        self.pullback_duration = 0
        else:
            self.pullback_active = False
            self.pullback_type = PullbackType.NONE
            self.pullback_max_depth = 0.0
            self.pullback_duration = 0

        # Check for nested pullback structure
        if self.pullback_active and self.pullback_duration > 5:
            minor_swings_in_pb = [s for s in (self.swing_highs + self.swing_lows) if s.index >= self.pullback_start_idx]
            if len(minor_swings_in_pb) >= 2:
                self.pullback_type = PullbackType.NESTED

        # 2. Recovery Detection
        if self.pullback_active and self.pullback_type in [PullbackType.HEALTHY, PullbackType.DEEP, PullbackType.NESTED]:
            offset = max(0, len(self.candles) - (self.global_index - self.pullback_start_idx + 1))
            if self.trend_direction == TrendDirection.BULLISH:
                pullback_low = min(c["low"] for c in self.candles[offset:])
                if curr_close > pullback_low:
                    if self.recovery_status in [RecoveryStatus.NONE, RecoveryStatus.FAILED]:
                        self.recovery_status = RecoveryStatus.ACTIVE
                        self.recovery_start_idx = self.global_index
                        self.recovery_highest_price = curr_close
                    
                    self.recovery_highest_price = max(self.recovery_highest_price, curr_close)
                    
                    p_sw_highs = [s for s in self.swing_highs if s.index >= self.pullback_start_idx]
                    if p_sw_highs and curr_close > p_sw_highs[-1].price:
                        self.recovery_status = RecoveryStatus.CONFIRMED
                    elif (self.global_index - self.recovery_start_idx) > 12:
                        self.recovery_status = RecoveryStatus.EXPIRED
                    elif curr_close < pullback_low:
                        self.recovery_status = RecoveryStatus.FAILED
            else:
                pullback_high = max(c["high"] for c in self.candles[offset:])
                if curr_close < pullback_high:
                    if self.recovery_status in [RecoveryStatus.NONE, RecoveryStatus.FAILED]:
                        self.recovery_status = RecoveryStatus.ACTIVE
                        self.recovery_start_idx = self.global_index
                        self.recovery_lowest_price = curr_close
                    
                    self.recovery_lowest_price = min(self.recovery_lowest_price, curr_close)
                    
                    p_sw_lows = [s for s in self.swing_lows if s.index >= self.pullback_start_idx]
                    if p_sw_lows and curr_close < p_sw_lows[-1].price:
                        self.recovery_status = RecoveryStatus.CONFIRMED
                    elif (self.global_index - self.recovery_start_idx) > 12:
                        self.recovery_status = RecoveryStatus.EXPIRED
                    elif curr_close > pullback_high:
                        self.recovery_status = RecoveryStatus.FAILED
        else:
            self.recovery_status = RecoveryStatus.NONE
            self.recovery_start_idx = -1

    def _get_pullback_metrics(self) -> PullbackMetrics:
        if not self.pullback_active:
            return PullbackMetrics(PullbackType.NONE, 0.0, 0.0, 0, 0.0)
            
        pb_vols = [c["volume"] for c in self.candles[-self.pullback_duration:]]
        avg_pb_vol = sum(pb_vols) / len(pb_vols) if pb_vols else 1.0
        
        hist_vols = [c["volume"] for c in self.candles[-30:]]
        avg_hist_vol = sum(hist_vols) / len(hist_vols) if hist_vols else 1.0
        
        quality = min(100.0, max(10.0, (avg_hist_vol / max(1.0, avg_pb_vol)) * 60.0))
        strength = min(100.0, self.pullback_max_depth)
        
        return PullbackMetrics(
            type=self.pullback_type, quality=quality, strength=strength,
            duration=self.pullback_duration, depth_pct=self.pullback_max_depth
        )

    def _get_recovery_metrics(self) -> RecoveryMetrics:
        if self.recovery_status == RecoveryStatus.NONE:
            return RecoveryMetrics(RecoveryStatus.NONE, 0.0, 0.0, 0.0)
            
        dur = self.global_index - self.recovery_start_idx + 1
        rec_candles = self.candles[-dur:]
        
        avg_rec_vol = sum(c["volume"] for c in rec_candles) / len(rec_candles) if rec_candles else 1.0
        
        pb_vols = [c["volume"] for c in self.candles[-max(1, dur+5):-dur]]
        avg_pb_vol = sum(pb_vols) / len(pb_vols) if pb_vols else 1.0
        
        quality = min(100.0, (avg_rec_vol / max(1.0, avg_pb_vol)) * 50.0)
        
        if rec_candles:
            strength = abs(rec_candles[-1]["close"] - rec_candles[0]["open"]) / max(1.0, dur)
            strength = min(100.0, strength * 200.0)
        else:
            strength = 0.0
            
        confidence = 50.0
        if self.trend_direction == TrendDirection.BULLISH:
            if self.support_levels:
                dist = min(abs(self.candles[-1]["close"] - s.price) / s.price for s in self.support_levels)
                if dist < 0.002:
                    confidence = 90.0
        else:
            if self.resistance_levels:
                dist = min(abs(self.candles[-1]["close"] - r.price) / r.price for r in self.resistance_levels)
                if dist < 0.002:
                    confidence = 90.0
                    
        return RecoveryMetrics(
            status=self.recovery_status, quality=quality, strength=strength, confidence=confidence
        )

    # --------------------------------------------------------------------------
    # LIQUIDITY ANALYSIS
    # --------------------------------------------------------------------------

    def _detect_liquidity(self):
        if len(self.candles) < 3:
            return
            
        curr_high = self.candles[-1]["high"]
        curr_low = self.candles[-1]["low"]
        curr_close = self.candles[-1]["close"]
        
        # 1. Equal Highs & Equal Lows (EQH / EQL)
        if len(self.swing_highs) >= 2:
            shs = self.swing_highs[-5:]
            for i in range(len(shs)):
                for j in range(i + 1, len(shs)):
                    if abs(shs[i].price - shs[j].price) / shs[i].price < 0.0015:
                        self._add_liquidity_event(LiquidityType.EQH, shs[j].price, "Equal highs detected.")
                        
        if len(self.swing_lows) >= 2:
            sls = self.swing_lows[-5:]
            for i in range(len(sls)):
                for j in range(i + 1, len(sls)):
                    if abs(sls[i].price - sls[j].price) / sls[i].price < 0.0015:
                        self._add_liquidity_event(LiquidityType.EQL, sls[j].price, "Equal lows detected.")

        # 2. Liquidity Sweeps & Stop Hunts (SFP)
        last_maj_h = self._get_last_major_swing(SwingType.HIGH)
        last_maj_l = self._get_last_major_swing(SwingType.LOW)
        
        if last_maj_h:
            if curr_high > last_maj_h.price and curr_close <= last_maj_h.price:
                sw_cand = self._get_candle_by_abs_idx(last_maj_h.index)
                if sw_cand and curr_close < max(sw_cand["open"], sw_cand["close"]):
                    self._add_liquidity_event(LiquidityType.SFP, last_maj_h.price, f"SFP High at {last_maj_h.price}")
                else:
                    self._add_liquidity_event(LiquidityType.SWEEP_HIGH, last_maj_h.price, f"Sweep High above {last_maj_h.price}")
                    
        if last_maj_l:
            if curr_low < last_maj_l.price and curr_close >= last_maj_l.price:
                sw_cand = self._get_candle_by_abs_idx(last_maj_l.index)
                if sw_cand and curr_close > min(sw_cand["open"], sw_cand["close"]):
                    self._add_liquidity_event(LiquidityType.SFP, last_maj_l.price, f"SFP Low at {last_maj_l.price}")
                else:
                    self._add_liquidity_event(LiquidityType.SWEEP_LOW, last_maj_l.price, f"Sweep Low below {last_maj_l.price}")

        # 3. False Breakouts / Breakout Traps
        if len(self.recent_breaks) > 0:
            last_break = self.recent_breaks[-1]
            if (self.global_index - last_break.index) <= 3:
                if last_break.direction == TrendDirection.BULLISH and curr_close < last_break.level:
                    self._add_liquidity_event(LiquidityType.FALSE_BREAKOUT, last_break.level, "Bullish False Breakout.")
                elif last_break.direction == TrendDirection.BEARISH and curr_close > last_break.level:
                    self._add_liquidity_event(LiquidityType.FALSE_BREAKOUT, last_break.level, "Bearish False Breakout.")

    def _add_liquidity_event(self, ltype: LiquidityType, price: float, desc: str):
        if self.liquidity_events and self.liquidity_events[-1].index == self.global_index and self.liquidity_events[-1].type == ltype:
            return
        self.liquidity_events.append(LiquidityEvent(
            index=self.global_index, type=ltype, price=price,
            volume=self.candles[-1]["volume"], description=desc
        ))
        if len(self.liquidity_events) > 20:
            self.liquidity_events.pop(0)

    # --------------------------------------------------------------------------
    # PRICE PATTERN DETECTION
    # --------------------------------------------------------------------------

    def _detect_price_patterns(self):
        swings = sorted(
            self.swing_highs + self.swing_lows,
            key=lambda x: x.index
        )
        if len(swings) < 3:
            return
            
        curr_close = self.candles[-1]["close"]
        
        def approx_eq(p1: float, p2: float) -> bool:
            return abs(p1 - p2) / p1 < 0.002
            
        # 1. Double Top / Bottom (last 3 swings)
        if len(swings) >= 3:
            s0, s1, s2 = swings[-3:]
            if s0.type == SwingType.HIGH and s1.type == SwingType.LOW and s2.type == SwingType.HIGH:
                if approx_eq(s0.price, s2.price):
                    status = PatternStatus.FORMING
                    if curr_close < s1.price:
                        status = PatternStatus.CONFIRMED
                    elif curr_close > max(s0.price, s2.price):
                        status = PatternStatus.BROKEN
                        
                    self._add_pattern(PricePattern(
                        type=PatternType.DOUBLE_TOP, direction=TrendDirection.BEARISH,
                        confidence=85.0 if status == PatternStatus.CONFIRMED else 50.0,
                        status=status, breakout_level=s1.price, invalidation_level=max(s0.price, s2.price),
                        age=self.global_index - s2.index
                    ))
                    
            elif s0.type == SwingType.LOW and s1.type == SwingType.HIGH and s2.type == SwingType.LOW:
                if approx_eq(s0.price, s2.price):
                    status = PatternStatus.FORMING
                    if curr_close > s1.price:
                        status = PatternStatus.CONFIRMED
                    elif curr_close < min(s0.price, s2.price):
                        status = PatternStatus.BROKEN
                        
                    self._add_pattern(PricePattern(
                        type=PatternType.DOUBLE_BOTTOM, direction=TrendDirection.BULLISH,
                        confidence=85.0 if status == PatternStatus.CONFIRMED else 50.0,
                        status=status, breakout_level=s1.price, invalidation_level=min(s0.price, s2.price),
                        age=self.global_index - s2.index
                    ))

        # 2. H&S and Triple Tops (last 5 swings)
        if len(swings) >= 5:
            s0, s1, s2, s3, s4 = swings[-5:]
            
            if (s0.type == SwingType.HIGH and s1.type == SwingType.LOW and 
                s2.type == SwingType.HIGH and s3.type == SwingType.LOW and 
                s4.type == SwingType.HIGH):
                if s2.price > s0.price and s2.price > s4.price:
                    neckline = max(s1.price, s3.price)
                    status = PatternStatus.FORMING
                    if curr_close < neckline:
                        status = PatternStatus.CONFIRMED
                    elif curr_close > s2.price:
                        status = PatternStatus.BROKEN
                        
                    self._add_pattern(PricePattern(
                        type=PatternType.HEAD_AND_SHOULDERS, direction=TrendDirection.BEARISH,
                        confidence=90.0 if status == PatternStatus.CONFIRMED else 60.0,
                        status=status, breakout_level=neckline, invalidation_level=s2.price,
                        age=self.global_index - s4.index
                    ))
                if approx_eq(s0.price, s2.price) and approx_eq(s2.price, s4.price):
                    neckline = min(s1.price, s3.price)
                    status = PatternStatus.FORMING
                    if curr_close < neckline:
                        status = PatternStatus.CONFIRMED
                    elif curr_close > max(s0.price, s2.price, s4.price):
                        status = PatternStatus.BROKEN
                    self._add_pattern(PricePattern(
                        type=PatternType.TRIPLE_TOP, direction=TrendDirection.BEARISH,
                        confidence=90.0 if status == PatternStatus.CONFIRMED else 60.0,
                        status=status, breakout_level=neckline, invalidation_level=max(s0.price, s2.price, s4.price),
                        age=self.global_index - s4.index
                    ))
                    
            elif (s0.type == SwingType.LOW and s1.type == SwingType.HIGH and 
                  s2.type == SwingType.LOW and s3.type == SwingType.HIGH and 
                  s4.type == SwingType.LOW):
                if s2.price < s0.price and s2.price < s4.price:
                    neckline = min(s1.price, s3.price)
                    status = PatternStatus.FORMING
                    if curr_close > neckline:
                        status = PatternStatus.CONFIRMED
                    elif curr_close < s2.price:
                        status = PatternStatus.BROKEN
                        
                    self._add_pattern(PricePattern(
                        type=PatternType.INVERSE_HEAD_AND_SHOULDERS, direction=TrendDirection.BULLISH,
                        confidence=90.0 if status == PatternStatus.CONFIRMED else 60.0,
                        status=status, breakout_level=neckline, invalidation_level=s2.price,
                        age=self.global_index - s4.index
                    ))
                if approx_eq(s0.price, s2.price) and approx_eq(s2.price, s4.price):
                    neckline = max(s1.price, s3.price)
                    status = PatternStatus.FORMING
                    if curr_close > neckline:
                        status = PatternStatus.CONFIRMED
                    elif curr_close < min(s0.price, s2.price, s4.price):
                        status = PatternStatus.BROKEN
                    self._add_pattern(PricePattern(
                        type=PatternType.TRIPLE_BOTTOM, direction=TrendDirection.BULLISH,
                        confidence=90.0 if status == PatternStatus.CONFIRMED else 60.0,
                        status=status, breakout_level=neckline, invalidation_level=min(s0.price, s2.price, s4.price),
                        age=self.global_index - s4.index
                    ))

        # Flags, Wedges, and Channels using linear slope fits on swings
        high_swings = [s for s in swings if s.type == SwingType.HIGH]
        low_swings = [s for s in swings if s.type == SwingType.LOW]
        
        if len(high_swings) >= 3 and len(low_swings) >= 3:
            h_prices = [h.price for h in high_swings[-3:]]
            h_indices = [h.index for h in high_swings[-3:]]
            l_prices = [l.price for l in low_swings[-3:]]
            l_indices = [l.index for l in low_swings[-3:]]
            
            h_slope = ((h_prices[-1] - h_prices[0]) / h_prices[0]) / max(1, h_indices[-1] - h_indices[0])
            l_slope = ((l_prices[-1] - l_prices[0]) / l_prices[0]) / max(1, l_indices[-1] - l_indices[0])
            
            if h_slope < -0.001 and l_slope > 0.001:
                status = PatternStatus.FORMING
                if curr_close > h_prices[-1]:
                    status = PatternStatus.CONFIRMED
                self._add_pattern(PricePattern(
                    type=PatternType.SYMMETRICAL_TRIANGLE, direction=TrendDirection.BULLISH,
                    confidence=70.0, status=status, breakout_level=h_prices[-1],
                    invalidation_level=l_prices[-1], age=self.global_index - high_swings[-1].index
                ))
            elif abs(h_slope) < 0.001 and l_slope > 0.001:
                status = PatternStatus.FORMING
                if curr_close > h_prices[-1]:
                    status = PatternStatus.CONFIRMED
                self._add_pattern(PricePattern(
                    type=PatternType.ASCENDING_TRIANGLE, direction=TrendDirection.BULLISH,
                    confidence=75.0, status=status, breakout_level=h_prices[-1],
                    invalidation_level=l_prices[-1], age=self.global_index - high_swings[-1].index
                ))
            elif h_slope < -0.001 and abs(l_slope) < 0.001:
                status = PatternStatus.FORMING
                if curr_close < l_prices[-1]:
                    status = PatternStatus.CONFIRMED
                self._add_pattern(PricePattern(
                    type=PatternType.DESCENDING_TRIANGLE, direction=TrendDirection.BEARISH,
                    confidence=75.0, status=status, breakout_level=l_prices[-1],
                    invalidation_level=h_prices[-1], age=self.global_index - low_swings[-1].index
                ))
            elif h_slope < -0.001 and l_slope < -0.001 and h_slope < l_slope:
                status = PatternStatus.FORMING
                if curr_close > h_prices[-1]:
                    status = PatternStatus.CONFIRMED
                self._add_pattern(PricePattern(
                    type=PatternType.FALLING_WEDGE, direction=TrendDirection.BULLISH,
                    confidence=80.0, status=status, breakout_level=h_prices[-1],
                    invalidation_level=l_prices[-1], age=self.global_index - high_swings[-1].index
                ))
            elif h_slope > 0.001 and l_slope > 0.001 and l_slope > h_slope:
                status = PatternStatus.FORMING
                if curr_close < l_prices[-1]:
                    status = PatternStatus.CONFIRMED
                self._add_pattern(PricePattern(
                    type=PatternType.RISING_WEDGE, direction=TrendDirection.BEARISH,
                    confidence=80.0, status=status, breakout_level=l_prices[-1],
                    invalidation_level=h_prices[-1], age=self.global_index - low_swings[-1].index
                ))
            elif h_slope > 0.001 and l_slope > 0.001 and abs(h_slope - l_slope) < 0.001:
                self._add_pattern(PricePattern(
                    type=PatternType.RISING_CHANNEL, direction=TrendDirection.BULLISH,
                    confidence=75.0, status=PatternStatus.FORMING, breakout_level=h_prices[-1],
                    invalidation_level=l_prices[-1], age=self.global_index - high_swings[-1].index
                ))
            elif h_slope < -0.001 and l_slope < -0.001 and abs(h_slope - l_slope) < 0.001:
                self._add_pattern(PricePattern(
                    type=PatternType.FALLING_CHANNEL, direction=TrendDirection.BEARISH,
                    confidence=75.0, status=PatternStatus.FORMING, breakout_level=h_prices[-1],
                    invalidation_level=l_prices[-1], age=self.global_index - high_swings[-1].index
                ))

        # Flags: Flagpole (strong movement) followed by tight consolidation channel
        if len(self.candles) >= 15:
            pole_candles = self.candles[-15:-5]
            low_pole = min(c["low"] for c in pole_candles)
            high_pole = max(c["high"] for c in pole_candles)
            pole_height = high_pole - low_pole
            
            if (pole_candles[-1]["close"] - pole_candles[0]["open"]) / pole_candles[0]["open"] > 0.03:
                cons_candles = self.candles[-5:]
                cons_high = max(c["high"] for c in cons_candles)
                cons_low = min(c["low"] for c in cons_candles)
                if (cons_high - cons_low) < (pole_height * 0.4):
                    status = PatternStatus.FORMING
                    if curr_close > cons_high:
                        status = PatternStatus.CONFIRMED
                    self._add_pattern(PricePattern(
                        type=PatternType.BULL_FLAG, direction=TrendDirection.BULLISH,
                        confidence=80.0, status=status, breakout_level=cons_high,
                        invalidation_level=cons_low, age=5
                    ))
            
            if (pole_candles[0]["open"] - pole_candles[-1]["close"]) / pole_candles[0]["open"] > 0.03:
                cons_candles = self.candles[-5:]
                cons_high = max(c["high"] for c in cons_candles)
                cons_low = min(c["low"] for c in cons_candles)
                if (cons_high - cons_low) < (pole_height * 0.4):
                    status = PatternStatus.FORMING
                    if curr_close < cons_low:
                        status = PatternStatus.CONFIRMED
                    self._add_pattern(PricePattern(
                        type=PatternType.BEAR_FLAG, direction=TrendDirection.BEARISH,
                        confidence=80.0, status=status, breakout_level=cons_low,
                        invalidation_level=cons_high, age=5
                    ))

    def _add_pattern(self, pattern: PricePattern):
        for idx, p in enumerate(self.patterns):
            if p.type == pattern.type:
                self.patterns[idx] = pattern
                return
        self.patterns.append(pattern)
        if len(self.patterns) > 20:
            self.patterns.pop(0)

    # --------------------------------------------------------------------------
    # MARKET PHASE TRANSITIONS
    # --------------------------------------------------------------------------

    def _determine_market_phase(self) -> MarketPhase:
        if not self.candles:
            return MarketPhase.RANGE
            
        curr_close = self.candles[-1]["close"]
        curr_open = self.candles[-1]["open"]
        
        # 1. Breakout Phase
        if self.recent_breaks:
            last_break = self.recent_breaks[-1]
            if (self.global_index - last_break.index) <= 2:
                return MarketPhase.BREAKOUT
                
        # 2. Reversal / Transition Phase
        if self.recent_breaks and self.recent_breaks[-1].type == BreakType.CHOCH:
            if (self.global_index - self.recent_breaks[-1].index) <= 5:
                return MarketPhase.TRANSITION

        # 3. Pullback and Recovery Phases
        if self.pullback_active:
            if self.recovery_status == RecoveryStatus.ACTIVE:
                return MarketPhase.RECOVERY
            return MarketPhase.PULLBACK
            
        # 4. Triangles/Wedges indicating Compression
        active_triangle = False
        for p in self.patterns[-3:]:
            if p.status == PatternStatus.FORMING and p.type in [
                PatternType.SYMMETRICAL_TRIANGLE, PatternType.ASCENDING_TRIANGLE,
                PatternType.DESCENDING_TRIANGLE, PatternType.RISING_WEDGE, PatternType.FALLING_WEDGE
            ]:
                active_triangle = True
                break
        if active_triangle:
            return MarketPhase.COMPRESSION
            
        # 5. Strong impulse candles (expansion)
        last_10 = self.candles[-10:]
        if len(last_10) >= 10:
            avg_body = sum(abs(c["close"] - c["open"]) for c in last_10) / 10.0
            avg_vol = sum(c["volume"] for c in last_10) / 10.0
            curr_body = abs(curr_close - curr_open)
            if curr_body > (avg_body * 2.0) and self.candles[-1]["volume"] > (avg_vol * 1.8):
                return MarketPhase.EXPANSION
            
        # 6. Trending Phase
        if self.trend_direction in [TrendDirection.BULLISH, TrendDirection.BEARISH]:
            return MarketPhase.TRENDING
            
        # 7. Accumulation & Distribution
        if self.trend_direction == TrendDirection.RANGING and len(self.candles) >= 50:
            prices = [c["close"] for c in self.candles[-50:]]
            max_p, min_p = max(prices), min(prices)
            rng_pct = (max_p - min_p) / min_p
            if rng_pct < 0.015:
                bearish_bias = self.candles[0]["close"] > self.candles[-1]["close"]
                if bearish_bias:
                    return MarketPhase.ACCUMULATION
                else:
                    return MarketPhase.DISTRIBUTION
                    
        return MarketPhase.RANGE

    # --------------------------------------------------------------------------
    # CONFIDENCE MODEL
    # --------------------------------------------------------------------------

    def _compute_confidence_score(self, phase: MarketPhase) -> float:
        scores = []
        
        # 1. Swing Quality
        swings = self.swing_highs[-3:] + self.swing_lows[-3:]
        if swings:
            swing_qual = sum(s.quality for s in swings) / len(swings)
            scores.append((swing_qual, 0.20))
        else:
            scores.append((50.0, 0.20))
            
        # 2. Trend Quality
        trend_score = 50.0
        if self.trend_direction in [TrendDirection.BULLISH, TrendDirection.BEARISH]:
            trend_score = 70.0 + min(30.0, self.consecutive_bos * 10.0)
            if self.trend_age > 40:
                trend_score = max(40.0, trend_score - (self.trend_age - 40) * 0.5)
        scores.append((trend_score, 0.20))
        
        # 3. Pullback / Recovery Quality
        pr_score = 100.0
        if self.pullback_active:
            p_metrics = self._get_pullback_metrics()
            r_metrics = self._get_recovery_metrics()
            if r_metrics.status == RecoveryStatus.ACTIVE:
                pr_score = (p_metrics.quality + r_metrics.quality) / 2.0
            else:
                pr_score = p_metrics.quality
        scores.append((pr_score, 0.20))
        
        # 4. Pattern Quality
        pattern_score = 50.0
        if self.patterns:
            recent_p = self.patterns[-1]
            if recent_p.status == PatternStatus.CONFIRMED:
                pattern_score = 90.0
            elif recent_p.status == PatternStatus.FORMING:
                pattern_score = 65.0
        scores.append((pattern_score, 0.15))
        
        # 5. Market Phase Clarity
        phase_score = 50.0
        if phase in [MarketPhase.TRENDING, MarketPhase.BREAKOUT, MarketPhase.EXPANSION]:
            phase_score = 90.0
        elif phase in [MarketPhase.PULLBACK, MarketPhase.RECOVERY, MarketPhase.COMPRESSION]:
            phase_score = 75.0
        elif phase == MarketPhase.RANGE:
            phase_score = 50.0
        elif phase in [MarketPhase.TRANSITION, MarketPhase.ACCUMULATION, MarketPhase.DISTRIBUTION]:
            phase_score = 40.0
        scores.append((phase_score, 0.15))
        
        # Weighted calculation
        total_weight = sum(w for _, w in scores)
        if total_weight > 0:
            final_score = sum(val * w for val, w in scores) / total_weight
        else:
            final_score = 50.0
            
        return max(0.0, min(100.0, final_score))

    # --------------------------------------------------------------------------
    # TREND METRICS COMPILATION
    # --------------------------------------------------------------------------

    def _get_trend_metrics(self) -> TrendMetrics:
        strength = 30.0
        if self.trend_direction in [TrendDirection.BULLISH, TrendDirection.BEARISH]:
            strength = min(100.0, 50.0 + self.consecutive_bos * 15.0)
            
        quality = 50.0
        if len(self.candles) >= 15:
            closes = [c["close"] for c in self.candles[-15:]]
            up_moves = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i-1])
            ratio = up_moves / 14.0
            if self.trend_direction == TrendDirection.BULLISH:
                quality = ratio * 100.0
            elif self.trend_direction == TrendDirection.BEARISH:
                quality = (1.0 - ratio) * 100.0
                
        momentum = 0.0
        if len(self.candles) >= 5:
            momentum = (self.candles[-1]["close"] - self.candles[-5]["close"]) / 5.0
            
        exhaustion = 0.0
        if self.trend_direction != TrendDirection.RANGING:
            exhaustion = min(60.0, (self.trend_age / 50.0) * 40.0)
            if self.trend_direction == TrendDirection.BULLISH and len(self.swing_highs) >= 2:
                sh = self.swing_highs[-2:]
                if sh[1].price > sh[0].price:
                    if len(self.swing_lows) >= 2:
                        sl = self.swing_lows[-2:]
                        prev_imp = sh[0].price - sl[0].price
                        curr_imp = sh[1].price - sl[1].price
                        if curr_imp < prev_imp:
                            exhaustion = min(100.0, exhaustion + 30.0)
            elif self.trend_direction == TrendDirection.BEARISH and len(self.swing_lows) >= 2:
                sl = self.swing_lows[-2:]
                if sl[1].price < sl[0].price:
                    if len(self.swing_highs) >= 2:
                        sh = self.swing_highs[-2:]
                        prev_imp = sh[0].price - sl[0].price
                        curr_imp = sh[1].price - sl[1].price
                        if curr_imp < prev_imp:
                            exhaustion = min(100.0, exhaustion + 30.0)
                            
        prob = (strength * 0.6) + (100.0 - exhaustion) * 0.4
        prob = max(10.0, min(95.0, prob))
            
        return TrendMetrics(
            direction=self.trend_direction, strength=strength, quality=quality,
            age=self.trend_age, momentum=momentum, exhaustion=exhaustion,
            continuation_probability=prob
        )


# ==============================================================================
# PUBLIC API WRAPPER
# ==============================================================================

def analyze(df: pd.DataFrame) -> StructureResult:
    """
    Analyzes option premium OHLCV DataFrame and returns the latest market structure.
    """
    engine = MarketStructureEngine()
    
    n_candles = len(df)
    if n_candles == 0:
        return StructureResult(
            swing_highs=[], swing_lows=[],
            trend=TrendMetrics(TrendDirection.RANGING, 0.0, 0.0, 0, 0.0, 0.0, 50.0),
            recent_breaks=[], protected_high=None, protected_low=None,
            support_levels=[], resistance_levels=[], swing_zones=[],
            pullback=PullbackMetrics(PullbackType.NONE, 0.0, 0.0, 0, 0.0),
            recovery=RecoveryMetrics(RecoveryStatus.NONE, 0.0, 0.0, 0.0),
            liquidity_events=[], patterns=[], phase=MarketPhase.RANGE, confidence_score=50.0
        )
        
    opens = df["open"].to_numpy(dtype=float)
    highs = df["high"].to_numpy(dtype=float)
    lows = df["low"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    volumes = df["volume"].to_numpy(dtype=float) if "volume" in df.columns else np.zeros(n_candles)
    times = df.index.to_numpy()
    
    # Process intermediate candles in fast mode (no SR touches, patterns, or confidence)
    for i in range(n_candles - 1):
        candle = {
            "open": opens[i],
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "volume": volumes[i],
            "time": times[i]
        }
        engine.update(candle, full_analysis=False)
        
    # Full analysis on the last candle only
    last_candle = {
        "open": opens[-1],
        "high": highs[-1],
        "low": lows[-1],
        "close": closes[-1],
        "volume": volumes[-1],
        "time": times[-1]
    }
    
    return engine.update(last_candle, full_analysis=True)
