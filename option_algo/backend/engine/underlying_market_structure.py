"""
Underlying Market Structure Engine for 5-Minute Indian Index Spot/Futures (NIFTY, BANKNIFTY, SENSEX)
Analyzes pure OHLCV price action to determine higher-timeframe directional bias for option buying.

This engine provides:
  – Higher Timeframe Trend & Direction
  – Major Market Structure (HH/HL/LH/LL)
  – BOS / CHOCH Detection
  – Protected High / Low
  – Dynamic Support & Resistance
  – Liquidity Analysis (EQH, EQL, Sweeps, SFP, False Breakouts)
  – Market Pattern Recognition (swing-based, not candle-based)
  – Market Phase Classification
  – Structural Confidence Score

Do NOT use: EMA, VWAP, ATR, ADX, RSI, MACD, Option Chain, Open Interest.
Do NOT generate BUY/SELL/CALL/PUT signals.

Usage:
    structure = underlying_market_structure.analyze(spot_5m_df)
    # returns UnderlyingStructureResult
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Tuple
import numpy as np
import pandas as pd


# ==============================================================================
# ENUMS
# ==============================================================================

class TrendDirection(Enum):
    """Primary market direction based on structural HH/HL/LH/LL analysis."""
    UPTREND = "UPTREND"
    DOWNTREND = "DOWNTREND"
    SIDEWAYS = "SIDEWAYS"
    TRANSITION = "TRANSITION"


class MarketPhase(Enum):
    """Current phase of the market cycle."""
    STRONG_TREND = "STRONG_TREND"
    WEAK_TREND = "WEAK_TREND"
    PULLBACK = "PULLBACK"
    BREAKOUT = "BREAKOUT"
    COMPRESSION = "COMPRESSION"
    EXPANSION = "EXPANSION"
    ACCUMULATION = "ACCUMULATION"
    DISTRIBUTION = "DISTRIBUTION"
    RANGE = "RANGE"
    TRANSITION = "TRANSITION"


class SwingType(Enum):
    """Type of swing point."""
    HIGH = "HIGH"
    LOW = "LOW"


class SwingLevel(Enum):
    """Classification of swing significance."""
    MAJOR = "MAJOR"
    MINOR = "MINOR"
    INTERNAL = "INTERNAL"


class MarketDirection(Enum):
    """Structural market direction based on HH/HL/LH/LL sequences."""
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    UNDEFINED = "UNDEFINED"


class BreakType(Enum):
    """Type of structural break."""
    BOS = "BOS"           # Break of Structure
    CHOCH = "CHOCH"       # Change of Character


class BreakoutStatus(Enum):
    """Confirmation status of a structural break."""
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"


class LiquidityType(Enum):
    """Type of liquidity event detected."""
    EQH = "EQH"                     # Equal Highs
    EQL = "EQL"                     # Equal Lows
    LIQUIDITY_SWEEP_HIGH = "LIQUIDITY_SWEEP_HIGH"
    LIQUIDITY_SWEEP_LOW = "LIQUIDITY_SWEEP_LOW"
    SFP_HIGH = "SFP_HIGH"           # Swing Failure Pattern at high
    SFP_LOW = "SFP_LOW"             # Swing Failure Pattern at low
    FALSE_BREAKOUT = "FALSE_BREAKOUT"
    BREAKOUT_TRAP = "BREAKOUT_TRAP"
    BUY_SIDE_LIQUIDITY = "BUY_SIDE_LIQUIDITY"
    SELL_SIDE_LIQUIDITY = "SELL_SIDE_LIQUIDITY"


class PatternType(Enum):
    """Recognized chart patterns based on swing geometry."""
    # Continuation Patterns
    BULL_FLAG = "BULL_FLAG"
    BEAR_FLAG = "BEAR_FLAG"
    ASCENDING_TRIANGLE = "ASCENDING_TRIANGLE"
    DESCENDING_TRIANGLE = "DESCENDING_TRIANGLE"
    SYMMETRICAL_TRIANGLE = "SYMMETRICAL_TRIANGLE"
    RISING_CHANNEL = "RISING_CHANNEL"
    FALLING_CHANNEL = "FALLING_CHANNEL"
    RECTANGLE = "RECTANGLE"
    # Reversal Patterns
    DOUBLE_TOP = "DOUBLE_TOP"
    DOUBLE_BOTTOM = "DOUBLE_BOTTOM"
    TRIPLE_TOP = "TRIPLE_TOP"
    TRIPLE_BOTTOM = "TRIPLE_BOTTOM"
    HEAD_AND_SHOULDERS = "HEAD_AND_SHOULDERS"
    INVERSE_HEAD_AND_SHOULDERS = "INVERSE_HEAD_AND_SHOULDERS"
    RISING_WEDGE = "RISING_WEDGE"
    FALLING_WEDGE = "FALLING_WEDGE"


class PatternState(Enum):
    """Current development state of a detected pattern."""
    DEVELOPING = "DEVELOPING"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"


# ==============================================================================
# DATACLASSES (Slots Enabled, Frozen/Immutable)
# ==============================================================================

@dataclass(slots=True, frozen=True)
class SwingPoint:
    """A detected swing high or low point."""
    index: int
    price: float
    swing_type: SwingType
    level: SwingLevel
    strength: float           # 0 to 100
    quality: float            # 0 to 100
    age: int                  # Bars since swing formed
    momentum: float           # Price momentum preceding the swing
    volume: float             # Volume at the swing candle


@dataclass(slots=True, frozen=True)
class TrendMetrics:
    """Structural trend analysis results."""
    direction: TrendDirection
    market_bias: MarketDirection
    strength: float           # 0 to 100
    quality: float            # 0 to 100
    age: int                  # Bars since last trend change
    momentum: float           # Average impulse leg slope
    exhaustion: float         # 0 to 100 — how mature/extended the trend is
    impulse_count: int        # Number of consecutive HH/HL or LH/LL sequences


@dataclass(slots=True, frozen=True)
class StructureBreak:
    """A detected Break of Structure or Change of Character."""
    index: int
    break_type: BreakType
    direction: MarketDirection
    level: float
    quality: float            # 0 to 100
    confidence: float         # 0 to 100
    status: BreakoutStatus
    retest_count: int         # Number of times level was retested after break


@dataclass(slots=True, frozen=True)
class PriceLevel:
    """A dynamically calculated support or resistance level."""
    price: float
    level_type: str           # "SUPPORT", "RESISTANCE", "DEMAND", "SUPPLY"
    strength: float           # 0 to 100
    touches: int
    age: int                  # Bars since level creation
    is_protected: bool


@dataclass(slots=True, frozen=True)
class SwingZone:
    """A demand or supply zone derived from a swing point."""
    level_start: float
    level_end: float
    zone_type: str            # "DEMAND" or "SUPPLY"
    mitigated: bool
    mitigation_index: Optional[int] = None
    strength: float = 50.0


@dataclass(slots=True, frozen=True)
class LiquidityEvent:
    """A detected institutional liquidity event."""
    index: int
    liquidity_type: LiquidityType
    price: float
    volume: float
    quality: float            # 0 to 100
    confidence: float         # 0 to 100
    description: str


@dataclass(slots=True, frozen=True)
class PricePattern:
    """A recognized chart pattern based on swing geometry."""
    pattern_type: PatternType
    direction: MarketDirection
    state: PatternState
    confidence: float         # 0 to 100
    quality: float            # 0 to 100
    breakout_level: float     # Level to break for confirmation
    invalidation_level: float # Level that invalidates the pattern
    age: int                  # Bars since pattern was first detected
    neckline: Optional[float] = None  # Neckline for H&S / double top patterns


@dataclass(slots=True, frozen=True)
class MarketPhaseResult:
    """Detailed market phase analysis."""
    phase: MarketPhase
    clarity: float            # 0 to 100 — how clearly defined the phase is
    description: str


@dataclass(slots=True, frozen=True)
class UnderlyingStructureResult:
    """
    Complete underlying market structure analysis result.
    This is the single output object returned by the public API.
    """
    trend: TrendDirection
    trend_strength: float
    trend_quality: float
    trend_age: int
    market_bias: MarketDirection
    market_phase: MarketPhase
    confidence: float
    major_swing_high: Optional[float]
    major_swing_low: Optional[float]
    support: Optional[float]
    resistance: Optional[float]
    protected_high: Optional[float]
    protected_low: Optional[float]
    last_bos: Optional[StructureBreak]
    last_choch: Optional[StructureBreak]
    active_pattern: Optional[PricePattern]
    pattern_state: Optional[PatternState]
    pattern_confidence: Optional[float]
    equal_high: Optional[float]
    equal_low: Optional[float]
    liquidity_event: Optional[LiquidityEvent]
    debug_information: dict


# ==============================================================================
# CORE STATE MACHINE ENGINE
# ==============================================================================

class UnderlyingMarketStructureEngine:
    """
    Incremental, constant-memory state machine for 5-minute underlying
    market structure analysis.

    Processes one candle at a time via update(). Maintains internal state
    arrays that are truncated to prevent unbounded memory growth.
    """

    def __init__(self, max_history: int = 500):
        self.max_history = max_history
        self.global_index = -1

        # Candle storage
        self.candles: List[dict] = []

        # Swing storage (truncated)
        self.swing_highs: List[SwingPoint] = []
        self.swing_lows: List[SwingPoint] = []

        # Structure breaks
        self.recent_breaks: List[StructureBreak] = []

        # Support / Resistance levels
        self.support_levels: List[PriceLevel] = []
        self.resistance_levels: List[PriceLevel] = []

        # Swing zones (demand / supply)
        self.swing_zones: List[SwingZone] = []

        # Liquidity events
        self.liquidity_events: List[LiquidityEvent] = []

        # Detected patterns
        self.patterns: List[PricePattern] = []

        # ─── Trend State Machine ─────────────────────────────────
        self.trend_direction = TrendDirection.SIDEWAYS
        self.market_bias = MarketDirection.UNDEFINED
        self.trend_age = 0
        self.impulse_count = 0
        self.consecutive_bullish_swings = 0  # consecutive HH + HL
        self.consecutive_bearish_swings = 0  # consecutive LH + LL

        # Protected structure
        self.protected_high: Optional[PriceLevel] = None
        self.protected_low: Optional[PriceLevel] = None

        # Structure sequence tracking
        self.last_structure_high: Optional[float] = None  # Last HH or LH
        self.last_structure_low: Optional[float] = None   # Last HL or LL
        self.last_structure_high_index: int = -1
        self.last_structure_low_index: int = -1
        self.structure_sequence: List[str] = []  # ["HH", "HL", "LH", "LL"]

        # Momentum tracking for trend quality
        self.impulse_leg_slopes: List[float] = []

        # Window sizes (tuned for 5-minute data)
        self.major_w = 5    # ~25 min confirmation for major swings
        self.minor_w = 2    # ~10 min confirmation for minor swings

        # Equal Highs/Lows tracking
        self.eqh_candidates: List[Tuple[float, int]] = []  # (price, index)
        self.eql_candidates: List[Tuple[float, int]] = []

    def update(self, candle: dict, full_analysis: bool = True) -> Optional[UnderlyingStructureResult]:
        """
        Processes a single candle incrementally.

        Args:
            candle: Dict with keys: open, high, low, close, volume
            full_analysis: If True, runs all analysis (S/R, patterns, confidence).
                          If False, only updates state machines (faster).

        Returns:
            UnderlyingStructureResult if full_analysis is True, else None.
        """
        self.global_index += 1
        self.candles.append(candle)

        # Maintain max history
        if len(self.candles) > self.max_history:
            self.candles.pop(0)

        # Update trend age
        if self.trend_direction != TrendDirection.SIDEWAYS:
            self.trend_age += 1

        # Step 1: Detect swings
        self._detect_swings()

        # Step 2: Update structure sequence
        self._update_structure_sequence()

        # Step 3: Detect BOS / CHOCH
        self._detect_structure_breaks()

        # Step 4: Mitigate swing zones
        self._update_zones_mitigation()

        # Step 5: Detect liquidity events
        self._detect_liquidity()

        if not full_analysis:
            return None

        # Step 6: Update S/R levels
        self._update_sr_levels()

        # Step 7: Detect price patterns
        self._detect_price_patterns()

        # Step 8: Determine market phase
        phase_result = self._determine_market_phase()

        # Step 9: Compute trend metrics and confidence
        trend_metrics = self._get_trend_metrics()
        confidence = self._compute_confidence_score(phase_result.phase)

        # Build result
        return self._build_result(trend_metrics, phase_result, confidence)

    # ==========================================================================
    # PUBLIC API: Analyze full DataFrame
    # ==========================================================================

    def analyze_full(self, df: pd.DataFrame) -> UnderlyingStructureResult:
        """
        Process an entire DataFrame and return the final analysis.

        Intermediate candles are processed in fast mode. Only the last
        candle triggers full analysis (S/R, patterns, confidence).
        """
        n = len(df)
        if n == 0:
            return self._empty_result()

        opens = df["open"].to_numpy(dtype=float)
        highs = df["high"].to_numpy(dtype=float)
        lows = df["low"].to_numpy(dtype=float)
        closes = df["close"].to_numpy(dtype=float)
        volumes = df["volume"].to_numpy(dtype=float) if "volume" in df.columns else np.zeros(n)
        times = df.index.to_numpy()

        # Process intermediate candles in fast mode
        for i in range(n - 1):
            candle = {
                "open": opens[i],
                "high": highs[i],
                "low": lows[i],
                "close": closes[i],
                "volume": volumes[i],
                "time": times[i],
            }
            self.update(candle, full_analysis=False)

        # Full analysis on the last candle
        last_candle = {
            "open": opens[-1],
            "high": highs[-1],
            "low": lows[-1],
            "close": closes[-1],
            "volume": volumes[-1],
            "time": times[-1],
        }
        return self.update(last_candle, full_analysis=True)

    # ==========================================================================
    # HELPER METHODS
    # ==========================================================================

    def _get_candle_by_abs_idx(self, abs_idx: int) -> Optional[dict]:
        """Get candle by absolute index, handling buffer offset."""
        offset = abs_idx - (self.global_index - len(self.candles) + 1)
        if 0 <= offset < len(self.candles):
            return self.candles[offset]
        return None

    def _is_swing_high(self, rel_idx: int, window: int) -> bool:
        """Check if candle at rel_idx (negative from end) is a swing high."""
        idx = len(self.candles) + rel_idx
        if not (0 <= idx < len(self.candles)):
            return False
        val = self.candles[idx]["high"]
        for offset in range(-window, window + 1):
            if offset == 0:
                continue
            check_idx = idx + offset
            if not (0 <= check_idx < len(self.candles)):
                return False
            if self.candles[check_idx]["high"] > val:
                return False
            # If equal high, prefer earlier candle (first occurrence)
            if self.candles[check_idx]["high"] == val and offset > 0:
                return False
        return True

    def _is_swing_low(self, rel_idx: int, window: int) -> bool:
        """Check if candle at rel_idx (negative from end) is a swing low."""
        idx = len(self.candles) + rel_idx
        if not (0 <= idx < len(self.candles)):
            return False
        val = self.candles[idx]["low"]
        for offset in range(-window, window + 1):
            if offset == 0:
                continue
            check_idx = idx + offset
            if not (0 <= check_idx < len(self.candles)):
                return False
            if self.candles[check_idx]["low"] < val:
                return False
            if self.candles[check_idx]["low"] == val and offset > 0:
                return False
        return True

    def _calculate_swing_metrics(self, abs_idx: int, swing_type: SwingType) -> Tuple[float, float, float]:
        """
        Calculate strength, quality, and momentum for a swing candle.

        Strength: How significant the swing is relative to recent price action
        Quality: Based on wick rejection volume confirmation
        Momentum: Preceding price slope
        """
        candle = self._get_candle_by_abs_idx(abs_idx)
        if candle is None:
            return 0.0, 0.0, 0.0

        rng = candle["high"] - candle["low"]

        # ── Quality: Wick rejection ──────────────────────────────
        wick_ratio = 0.0
        if rng > 0:
            if swing_type == SwingType.HIGH:
                # Upper wick rejection: price was pushed up then rejected
                upper_wick = candle["high"] - max(candle["open"], candle["close"])
                wick_ratio = upper_wick / rng
            else:
                # Lower wick rejection: price was pushed down then rejected
                lower_wick = min(candle["open"], candle["close"]) - candle["low"]
                wick_ratio = lower_wick / rng

        quality = min(100.0, wick_ratio * 150.0)

        # ── Quality: Volume confirmation ─────────────────────────
        vol = candle["volume"]
        buffer_offset = abs_idx - (self.global_index - len(self.candles) + 1)
        avg_vol = 1.0
        if buffer_offset > 0:
            slice_start = max(0, buffer_offset - 10)
            prev_vols = [c["volume"] for c in self.candles[slice_start:buffer_offset]]
            if prev_vols:
                avg_vol = sum(prev_vols) / len(prev_vols)

        vol_ratio = vol / max(1.0, avg_vol)
        quality = min(100.0, quality + min(50.0, (vol_ratio - 1.0) * 25.0))

        # ── Momentum: Preceding price slope ──────────────────────
        prev_candle = self._get_candle_by_abs_idx(abs_idx - 3)
        momentum = 0.0
        if prev_candle:
            momentum = (candle["close"] - prev_candle["close"]) / 3.0

        # ── Strength: Distance from current price ────────────────
        curr_price = self.candles[-1]["close"]
        if swing_type == SwingType.HIGH:
            price_diff = candle["high"] - curr_price  # positive if price is below
        else:
            price_diff = curr_price - candle["low"]   # positive if price is above

        strength = min(100.0, max(0.0, (price_diff / max(0.1, candle["close"])) * 500.0))

        return strength, quality, momentum

    def _get_last_major_swing(self, swing_type: SwingType) -> Optional[SwingPoint]:
        """Get the most recent MAJOR swing of the given type."""
        arr = self.swing_highs if swing_type == SwingType.HIGH else self.swing_lows
        majors = [s for s in arr if s.level == SwingLevel.MAJOR]
        return majors[-1] if majors else None

    def _get_last_swing(self, swing_type: SwingType) -> Optional[SwingPoint]:
        """Get the most recent swing of the given type (any level)."""
        arr = self.swing_highs if swing_type == SwingType.HIGH else self.swing_lows
        return arr[-1] if arr else None

    def _is_price_above_all_swing_lows(self) -> bool:
        """Check if current close is above all major swing lows."""
        if not self.swing_lows:
            return True
        curr_close = self.candles[-1]["close"]
        return all(curr_close > sw.price for sw in self.swing_lows if sw.level == SwingLevel.MAJOR)

    def _is_price_below_all_swing_highs(self) -> bool:
        """Check if current close is below all major swing highs."""
        if not self.swing_highs:
            return True
        curr_close = self.candles[-1]["close"]
        return all(curr_close < sw.price for sw in self.swing_highs if sw.level == SwingLevel.MAJOR)

    def _get_avg_body_ratio(self, n: int = 10) -> float:
        """Average candle body as ratio of range over last n candles."""
        if len(self.candles) < n:
            return 0.5
        ratios = []
        for c in self.candles[-n:]:
            rng = c["high"] - c["low"]
            if rng > 0:
                ratios.append(abs(c["close"] - c["open"]) / rng)
        return sum(ratios) / len(ratios) if ratios else 0.5

    def _get_volatility(self, n: int = 20) -> float:
        """Average candle range as percentage over last n candles."""
        if len(self.candles) < n:
            return 0.0
        ranges = []
        for c in self.candles[-n:]:
            ranges.append((c["high"] - c["low"]) / max(c["close"], 1.0))
        return sum(ranges) / len(ranges) if ranges else 0.0

    # ==========================================================================
    # 1. SWING DETECTION
    # ==========================================================================

    def _detect_swings(self):
        """Detect and register minor and major swing points."""
        # ── Minor swings (10-minute confirmation on 5m data) ────
        minor_idx = self.global_index - self.minor_w
        if minor_idx >= self.minor_w:
            # Minor swing high
            if self._is_swing_high(-1 - self.minor_w, self.minor_w):
                candle = self._get_candle_by_abs_idx(minor_idx)
                if candle and not any(s.index == minor_idx for s in self.swing_highs):
                    # Determine level
                    level = SwingLevel.MINOR
                    last_maj_h = self._get_last_major_swing(SwingType.HIGH)
                    last_maj_l = self._get_last_major_swing(SwingType.LOW)
                    if last_maj_h and last_maj_l:
                        if last_maj_l.price < candle["high"] < last_maj_h.price:
                            level = SwingLevel.INTERNAL

                    strength, quality, momentum = self._calculate_swing_metrics(
                        minor_idx, SwingType.HIGH
                    )
                    swing = SwingPoint(
                        index=minor_idx,
                        price=candle["high"],
                        swing_type=SwingType.HIGH,
                        level=level,
                        strength=strength,
                        quality=quality,
                        age=self.minor_w,
                        momentum=momentum,
                        volume=candle["volume"],
                    )
                    self._add_swing_high(swing)

                    # Create supply zone
                    self.swing_zones.append(SwingZone(
                        level_start=min(candle["open"], candle["close"]),
                        level_end=candle["high"],
                        zone_type="SUPPLY",
                        mitigated=False,
                        strength=quality,
                    ))

            # Minor swing low
            if self._is_swing_low(-1 - self.minor_w, self.minor_w):
                candle = self._get_candle_by_abs_idx(minor_idx)
                if candle and not any(s.index == minor_idx for s in self.swing_lows):
                    level = SwingLevel.MINOR
                    last_maj_h = self._get_last_major_swing(SwingType.HIGH)
                    last_maj_l = self._get_last_major_swing(SwingType.LOW)
                    if last_maj_h and last_maj_l:
                        if last_maj_l.price < candle["low"] < last_maj_h.price:
                            level = SwingLevel.INTERNAL

                    strength, quality, momentum = self._calculate_swing_metrics(
                        minor_idx, SwingType.LOW
                    )
                    swing = SwingPoint(
                        index=minor_idx,
                        price=candle["low"],
                        swing_type=SwingType.LOW,
                        level=level,
                        strength=strength,
                        quality=quality,
                        age=self.minor_w,
                        momentum=momentum,
                        volume=candle["volume"],
                    )
                    self._add_swing_low(swing)

                    # Create demand zone
                    self.swing_zones.append(SwingZone(
                        level_start=candle["low"],
                        level_end=max(candle["open"], candle["close"]),
                        zone_type="DEMAND",
                        mitigated=False,
                        strength=quality,
                    ))

        # ── Major swings (25-minute confirmation on 5m data) ────
        major_idx = self.global_index - self.major_w
        if major_idx >= self.major_w:
            # Major swing high
            if self._is_swing_high(-1 - self.major_w, self.major_w):
                candle = self._get_candle_by_abs_idx(major_idx)
                if candle:
                    # Check if already exists as minor, upgrade it
                    found = False
                    for i, sw in enumerate(self.swing_highs):
                        if sw.index == major_idx:
                            self.swing_highs[i] = SwingPoint(
                                index=sw.index,
                                price=sw.price,
                                swing_type=SwingType.HIGH,
                                level=SwingLevel.MAJOR,
                                strength=sw.strength,
                                quality=sw.quality,
                                age=sw.age,
                                momentum=sw.momentum,
                                volume=sw.volume,
                            )
                            found = True
                            break

                    if not found:
                        strength, quality, momentum = self._calculate_swing_metrics(
                            major_idx, SwingType.HIGH
                        )
                        swing = SwingPoint(
                            index=major_idx,
                            price=candle["high"],
                            swing_type=SwingType.HIGH,
                            level=SwingLevel.MAJOR,
                            strength=strength,
                            quality=quality,
                            age=self.major_w,
                            momentum=momentum,
                            volume=candle["volume"],
                        )
                        self._add_swing_high(swing)

            # Major swing low
            if self._is_swing_low(-1 - self.major_w, self.major_w):
                candle = self._get_candle_by_abs_idx(major_idx)
                if candle:
                    found = False
                    for i, sw in enumerate(self.swing_lows):
                        if sw.index == major_idx:
                            self.swing_lows[i] = SwingPoint(
                                index=sw.index,
                                price=sw.price,
                                swing_type=SwingType.LOW,
                                level=SwingLevel.MAJOR,
                                strength=sw.strength,
                                quality=sw.quality,
                                age=sw.age,
                                momentum=sw.momentum,
                                volume=sw.volume,
                            )
                            found = True
                            break

                    if not found:
                        strength, quality, momentum = self._calculate_swing_metrics(
                            major_idx, SwingType.LOW
                        )
                        swing = SwingPoint(
                            index=major_idx,
                            price=candle["low"],
                            swing_type=SwingType.LOW,
                            level=SwingLevel.MAJOR,
                            strength=strength,
                            quality=quality,
                            age=self.major_w,
                            momentum=momentum,
                            volume=candle["volume"],
                        )
                        self._add_swing_low(swing)

    def _add_swing_high(self, swing: SwingPoint):
        """Register a swing high, truncating list to limit memory."""
        self.swing_highs.append(swing)
        if len(self.swing_highs) > 30:
            self.swing_highs.pop(0)

    def _add_swing_low(self, swing: SwingPoint):
        """Register a swing low, truncating list to limit memory."""
        self.swing_lows.append(swing)
        if len(self.swing_lows) > 30:
            self.swing_lows.pop(0)

    # ==========================================================================
    # 2. STRUCTURE SEQUENCE TRACKING (HH/HL/LH/LL)
    # ==========================================================================

    def _update_structure_sequence(self):
        """
        Track the sequential relationship between swings to identify
        Higher High (HH), Higher Low (HL), Lower High (LH), Lower Low (LL).
        """
        if len(self.swing_highs) < 2 or len(self.swing_lows) < 2:
            return

        # Get the last two swing highs and lows
        sh2 = self.swing_highs[-2] if len(self.swing_highs) >= 2 else None
        sh1 = self.swing_highs[-1] if len(self.swing_highs) >= 1 else None
        sl2 = self.swing_lows[-2] if len(self.swing_lows) >= 2 else None
        sl1 = self.swing_lows[-1] if len(self.swing_lows) >= 1 else None

        # Determine the order of the last two swings
        all_swings = sorted(
            self.swing_highs[-2:] + self.swing_lows[-2:],
            key=lambda x: x.index,
        )

        if len(all_swings) < 2:
            return

        last = all_swings[-1]
        prev = all_swings[-2]

        # Classify the last swing relative to the previous one of the same type
        if last.swing_type == SwingType.HIGH and sh2 is not None and sh1 is not None:
            if sh1.index == last.index:  # sh1 is the latest high
                if abs(sh1.price - sh2.price) / max(sh2.price, 1.0) > 0.0005:
                    if sh1.price > sh2.price:
                        self.structure_sequence.append("HH")
                        self.consecutive_bullish_swings += 1
                        self.consecutive_bearish_swings = 0
                    else:
                        self.structure_sequence.append("LH")
                        self.consecutive_bearish_swings += 1
                        self.consecutive_bullish_swings = 0

        elif last.swing_type == SwingType.LOW and sl2 is not None and sl1 is not None:
            if sl1.index == last.index:  # sl1 is the latest low
                if abs(sl1.price - sl2.price) / max(sl2.price, 1.0) > 0.0005:
                    if sl1.price > sl2.price:
                        self.structure_sequence.append("HL")
                        self.consecutive_bullish_swings += 1
                        self.consecutive_bearish_swings = 0
                    else:
                        self.structure_sequence.append("LL")
                        self.consecutive_bearish_swings += 1
                        self.consecutive_bullish_swings = 0

        # Keep only last 10 sequence entries
        if len(self.structure_sequence) > 20:
            self.structure_sequence = self.structure_sequence[-20:]

        # Update last structure high/low
        if sh1 is not None:
            self.last_structure_high = sh1.price
            self.last_structure_high_index = sh1.index
        if sl1 is not None:
            self.last_structure_low = sl1.price
            self.last_structure_low_index = sl1.index

    # ==========================================================================
    # 3. TREND ENGINE
    # ==========================================================================

    def _get_trend_metrics(self) -> TrendMetrics:
        """
        Determine trend using ONLY structural swing relationships.
        No technical indicators used.

        Bullish Trend: At least 2 consecutive HH+HL sequences
        Bearish Trend: At least 2 consecutive LH+LL sequences
        Sideways: Mixed or unclear sequence
        Transition: Recent CHOCH
        """
        # ── Trend Direction ──────────────────────────────────────
        direction = TrendDirection.SIDEWAYS
        market_bias = MarketDirection.UNDEFINED

        # Check for recent CHOCH (transition)
        recent_choch = None
        for brk in reversed(self.recent_breaks):
            if brk.break_type == BreakType.CHOCH:
                if (self.global_index - brk.index) <= 8:
                    recent_choch = brk
                    break

        if recent_choch is not None:
            direction = TrendDirection.TRANSITION
            market_bias = recent_choch.direction
        elif self.consecutive_bullish_swings >= 3:
            direction = TrendDirection.UPTREND
            market_bias = MarketDirection.BULLISH
        elif self.consecutive_bearish_swings >= 3:
            direction = TrendDirection.DOWNTREND
            market_bias = MarketDirection.BEARISH
        elif self.consecutive_bullish_swings >= 1:
            direction = TrendDirection.UPTREND
            market_bias = MarketDirection.BULLISH
        elif self.consecutive_bearish_swings >= 1:
            direction = TrendDirection.DOWNTREND
            market_bias = MarketDirection.BEARISH
        else:
            direction = TrendDirection.SIDEWAYS
            market_bias = MarketDirection.UNDEFINED

        # Update engine state
        if direction != self.trend_direction:
            self.trend_direction = direction
            self.trend_age = 0
        self.market_bias = market_bias

        # ── Trend Strength (0-100) ───────────────────────────────
        strength = 30.0
        if direction == TrendDirection.UPTREND:
            strength = min(100.0, 40.0 + self.consecutive_bullish_swings * 12.0)
        elif direction == TrendDirection.DOWNTREND:
            strength = min(100.0, 40.0 + self.consecutive_bearish_swings * 12.0)
        elif direction == TrendDirection.TRANSITION:
            if recent_choch:
                strength = 60.0 if recent_choch.confidence > 60 else 40.0

        # ── Trend Quality (0-100) ────────────────────────────────
        quality = 50.0
        if len(self.candles) >= 15:
            closes = [c["close"] for c in self.candles[-15:]]
            up_moves = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i - 1])
            ratio = up_moves / 14.0
            if direction == TrendDirection.UPTREND:
                quality = ratio * 100.0
            elif direction == TrendDirection.DOWNTREND:
                quality = (1.0 - ratio) * 100.0
            elif direction == TrendDirection.SIDEWAYS:
                quality = 50.0

        # ── Trend Momentum ───────────────────────────────────────
        momentum = 0.0
        if len(self.candles) >= 5:
            momentum = (self.candles[-1]["close"] - self.candles[-5]["close"]) / 5.0

        # ── Trend Exhaustion (0-100) ─────────────────────────────
        exhaustion = 0.0
        if direction in (TrendDirection.UPTREND, TrendDirection.DOWNTREND):
            # Older trends tend toward exhaustion
            exhaustion = min(60.0, (self.trend_age / 50.0) * 40.0)

            # Check for diminishing impulse legs (divergence)
            if direction == TrendDirection.UPTREND and len(self.swing_highs) >= 2:
                sh = self.swing_highs[-2:]
                if sh[1].price > sh[0].price and len(self.swing_lows) >= 2:
                    sl = self.swing_lows[-2:]
                    prev_impulse = sh[0].price - sl[0].price
                    curr_impulse = sh[1].price - sl[1].price
                    if curr_impulse < prev_impulse:
                        exhaustion = min(100.0, exhaustion + 30.0)
            elif direction == TrendDirection.DOWNTREND and len(self.swing_lows) >= 2:
                sl = self.swing_lows[-2:]
                if sl[1].price < sl[0].price and len(self.swing_highs) >= 2:
                    sh = self.swing_highs[-2:]
                    prev_impulse = sh[0].price - sl[0].price
                    curr_impulse = sh[1].price - sl[1].price
                    if curr_impulse < prev_impulse:
                        exhaustion = min(100.0, exhaustion + 30.0)

        # ── Impulse count ────────────────────────────────────────
        impulse_count = max(self.consecutive_bullish_swings, self.consecutive_bearish_swings)

        return TrendMetrics(
            direction=direction,
            market_bias=market_bias,
            strength=strength,
            quality=quality,
            age=self.trend_age,
            momentum=momentum,
            exhaustion=exhaustion,
            impulse_count=impulse_count,
        )

    # ==========================================================================
    # 4. BOS & CHOCH DETECTION
    # ==========================================================================

    def _detect_structure_breaks(self):
        """
        Detect Break of Structure (BOS) and Change of Character (CHOCH).

        Bullish BOS: Price breaks above the most recent major swing high.
        Bearish BOS: Price breaks below the most recent major swing low.
        Bullish CHOCH: During downtrend, price breaks above protected high.
        Bearish CHOCH: During uptrend, price breaks below protected low.
        """
        if not self.candles:
            return

        curr_close = self.candles[-1]["close"]
        curr_high = self.candles[-1]["high"]
        curr_low = self.candles[-1]["low"]
        last_maj_h = self._get_last_major_swing(SwingType.HIGH)
        last_maj_l = self._get_last_major_swing(SwingType.LOW)

        if not last_maj_h or not last_maj_l:
            return

        # Ensure trend state is current
        trend = self._get_trend_metrics()
        current_direction = trend.direction
        current_bias = trend.market_bias

        # ── BOS Detection: Price breaks a major swing point ──────
        # Bullish BOS: Close above last major swing high
        if curr_close > last_maj_h.price:
            # Check we haven't already recorded this break
            already_recorded = False
            for brk in self.recent_breaks[-3:]:
                if brk.break_type == BreakType.BOS and brk.direction == MarketDirection.BULLISH:
                    if abs(brk.level - last_maj_h.price) / max(last_maj_h.price, 1.0) < 0.001:
                        already_recorded = True
                        break

            if not already_recorded:
                # Calculate break quality
                break_distance = (curr_close - last_maj_h.price) / max(last_maj_h.price, 1.0)
                quality = min(100.0, break_distance * 10000.0)  # 1% = 100 quality

                # Confidence based on volume and candle body
                vol_ratio = 1.0
                buffer_offset = self.global_index - (self.global_index - len(self.candles) + 1)
                if buffer_offset > 0:
                    slice_start = max(0, buffer_offset - 10)
                    prev_vols = [c["volume"] for c in self.candles[slice_start:buffer_offset]]
                    if prev_vols:
                        avg_prev_vol = sum(prev_vols) / len(prev_vols)
                        if avg_prev_vol > 0:
                            vol_ratio = self.candles[-1]["volume"] / avg_prev_vol

                rng = self.candles[-1]["high"] - self.candles[-1]["low"]
                body = abs(self.candles[-1]["close"] - self.candles[-1]["open"])
                body_ratio = body / rng if rng > 0 else 0.5

                confidence = min(100.0, quality * 0.5 + vol_ratio * 20.0 + body_ratio * 20.0)

                status = BreakoutStatus.CONFIRMED
                if body_ratio < 0.3 or vol_ratio < 0.8:
                    status = BreakoutStatus.PENDING

                bos = StructureBreak(
                    index=self.global_index,
                    break_type=BreakType.BOS,
                    direction=MarketDirection.BULLISH,
                    level=last_maj_h.price,
                    quality=quality,
                    confidence=confidence,
                    status=status,
                    retest_count=0,
                )
                self.recent_breaks.append(bos)

                # Update protected low: the low that preceded this breakout
                self._update_protected_low(last_maj_h.index)

                # Track impulse leg slope
                if len(self.swing_lows) >= 1:
                    last_sw_low = self._get_last_swing(SwingType.LOW)
                    if last_sw_low:
                        slope = (curr_close - last_sw_low.price) / max(
                            1, self.global_index - last_sw_low.index
                        )
                        self.impulse_leg_slopes.append(slope)

        # Bearish BOS: Close below last major swing low
        elif curr_close < last_maj_l.price:
            already_recorded = False
            for brk in self.recent_breaks[-3:]:
                if brk.break_type == BreakType.BOS and brk.direction == MarketDirection.BEARISH:
                    if abs(brk.level - last_maj_l.price) / max(last_maj_l.price, 1.0) < 0.001:
                        already_recorded = True
                        break

            if not already_recorded:
                break_distance = (last_maj_l.price - curr_close) / max(last_maj_l.price, 1.0)
                quality = min(100.0, break_distance * 10000.0)

                vol_ratio = 1.0
                buffer_offset = self.global_index - (self.global_index - len(self.candles) + 1)
                if buffer_offset > 0:
                    slice_start = max(0, buffer_offset - 10)
                    prev_vols = [c["volume"] for c in self.candles[slice_start:buffer_offset]]
                    if prev_vols:
                        avg_prev_vol = sum(prev_vols) / len(prev_vols)
                        if avg_prev_vol > 0:
                            vol_ratio = self.candles[-1]["volume"] / avg_prev_vol

                rng = self.candles[-1]["high"] - self.candles[-1]["low"]
                body = abs(self.candles[-1]["close"] - self.candles[-1]["open"])
                body_ratio = body / rng if rng > 0 else 0.5

                confidence = min(100.0, quality * 0.5 + vol_ratio * 20.0 + body_ratio * 20.0)

                status = BreakoutStatus.CONFIRMED
                if body_ratio < 0.3 or vol_ratio < 0.8:
                    status = BreakoutStatus.PENDING

                bos = StructureBreak(
                    index=self.global_index,
                    break_type=BreakType.BOS,
                    direction=MarketDirection.BEARISH,
                    level=last_maj_l.price,
                    quality=quality,
                    confidence=confidence,
                    status=status,
                    retest_count=0,
                )
                self.recent_breaks.append(bos)

                self._update_protected_high(last_maj_l.index)

                if len(self.swing_highs) >= 1:
                    last_sw_high = self._get_last_swing(SwingType.HIGH)
                    if last_sw_high:
                        slope = (last_sw_high.price - curr_close) / max(
                            1, self.global_index - last_sw_high.index
                        )
                        self.impulse_leg_slopes.append(slope)

        # ── CHOCH Detection: Change of Character ─────────────────
        # Bearish CHOCH: During uptrend, close breaks below protected low
        if current_bias == MarketDirection.BULLISH and self.protected_low is not None:
            if curr_close < self.protected_low.price:
                already_recorded = False
                for brk in self.recent_breaks[-3:]:
                    if brk.break_type == BreakType.CHOCH and brk.direction == MarketDirection.BEARISH:
                        if abs(brk.level - self.protected_low.price) / max(self.protected_low.price, 1.0) < 0.001:
                            already_recorded = True
                            break

                if not already_recorded:
                    quality = 80.0  # CHOCH is always significant
                    confidence = 85.0 if self.candles[-1]["volume"] > 1.5 * (
                        sum(c["volume"] for c in self.candles[-10:-1]) / 9.0
                    ) else 70.0

                    choch = StructureBreak(
                        index=self.global_index,
                        break_type=BreakType.CHOCH,
                        direction=MarketDirection.BEARISH,
                        level=self.protected_low.price,
                        quality=quality,
                        confidence=confidence,
                        status=BreakoutStatus.CONFIRMED,
                        retest_count=0,
                    )
                    self.recent_breaks.append(choch)

                    # Reset trend state
                    self.consecutive_bullish_swings = 0

        # Bullish CHOCH: During downtrend, close breaks above protected high
        elif current_bias == MarketDirection.BEARISH and self.protected_high is not None:
            if curr_close > self.protected_high.price:
                already_recorded = False
                for brk in self.recent_breaks[-3:]:
                    if brk.break_type == BreakType.CHOCH and brk.direction == MarketDirection.BULLISH:
                        if abs(brk.level - self.protected_high.price) / max(self.protected_high.price, 1.0) < 0.001:
                            already_recorded = True
                            break

                if not already_recorded:
                    quality = 80.0
                    confidence = 85.0 if self.candles[-1]["volume"] > 1.5 * (
                        sum(c["volume"] for c in self.candles[-10:-1]) / 9.0
                    ) else 70.0

                    choch = StructureBreak(
                        index=self.global_index,
                        break_type=BreakType.CHOCH,
                        direction=MarketDirection.BULLISH,
                        level=self.protected_high.price,
                        quality=quality,
                        confidence=confidence,
                        status=BreakoutStatus.CONFIRMED,
                        retest_count=0,
                    )
                    self.recent_breaks.append(choch)

                    self.consecutive_bearish_swings = 0

        # ── Failed BOS Detection ─────────────────────────────────
        # If a break happened within last 5 bars but price reversed back
        for brk in self.recent_breaks[-5:]:
            if brk.status == BreakoutStatus.PENDING or brk.status == BreakoutStatus.CONFIRMED:
                if (self.global_index - brk.index) <= 5:
                    if brk.direction == MarketDirection.BULLISH:
                        # Bullish break failed: price fell back below
                        if curr_close < brk.level:
                            # Update existing break to FAILED
                            for i, b in enumerate(self.recent_breaks):
                                if b.index == brk.index and b.break_type == brk.break_type:
                                    self.recent_breaks[i] = StructureBreak(
                                        index=b.index,
                                        break_type=b.break_type,
                                        direction=b.direction,
                                        level=b.level,
                                        quality=b.quality,
                                        confidence=20.0,  # Low confidence after failure
                                        status=BreakoutStatus.FAILED,
                                        retest_count=b.retest_count,
                                    )
                                    break
                    elif brk.direction == MarketDirection.BEARISH:
                        # Bearish break failed: price rallied back above
                        if curr_close > brk.level:
                            for i, b in enumerate(self.recent_breaks):
                                if b.index == brk.index and b.break_type == brk.break_type:
                                    self.recent_breaks[i] = StructureBreak(
                                        index=b.index,
                                        break_type=b.break_type,
                                        direction=b.direction,
                                        level=b.level,
                                        quality=b.quality,
                                        confidence=20.0,
                                        status=BreakoutStatus.FAILED,
                                        retest_count=b.retest_count,
                                    )
                                    break

        # Truncate breaks list
        if len(self.recent_breaks) > 30:
            self.recent_breaks = self.recent_breaks[-20:]

    # ==========================================================================
    # 5. PROTECTED STRUCTURE
    # ==========================================================================

    def _update_protected_low(self, break_origin_idx: int):
        """
        Update the protected low based on the lowest swing low before
        the break origin. This becomes the structural invalidation point.
        """
        if not self.swing_lows:
            return
        lowest_low = None
        for sw in reversed(self.swing_lows):
            if sw.index <= break_origin_idx:
                if lowest_low is None or sw.price < lowest_low.price:
                    lowest_low = sw

        if lowest_low:
            self.protected_low = PriceLevel(
                price=lowest_low.price,
                level_type="SUPPORT",
                strength=100.0,
                touches=1,
                age=self.global_index - lowest_low.index,
                is_protected=True,
            )

    def _update_protected_high(self, break_origin_idx: int):
        """
        Update the protected high based on the highest swing high before
        the break origin. This becomes the structural invalidation point.
        """
        if not self.swing_highs:
            return
        highest_high = None
        for sw in reversed(self.swing_highs):
            if sw.index <= break_origin_idx:
                if highest_high is None or sw.price > highest_high.price:
                    highest_high = sw

        if highest_high:
            self.protected_high = PriceLevel(
                price=highest_high.price,
                level_type="RESISTANCE",
                strength=100.0,
                touches=1,
                age=self.global_index - highest_high.index,
                is_protected=True,
            )

    # ==========================================================================
    # 6. DYNAMIC SUPPORT & RESISTANCE
    # ==========================================================================

    def _update_zones_mitigation(self):
        """Mark swing zones as mitigated when price reaches them."""
        if not self.candles:
            return
        curr_close = self.candles[-1]["close"]
        for idx in range(len(self.swing_zones)):
            zone = self.swing_zones[idx]
            if not zone.mitigated:
                if zone.zone_type == "SUPPLY" and curr_close > zone.level_end:
                    self.swing_zones[idx] = SwingZone(
                        level_start=zone.level_start,
                        level_end=zone.level_end,
                        zone_type=zone.zone_type,
                        mitigated=True,
                        mitigation_index=self.global_index,
                        strength=zone.strength,
                    )
                elif zone.zone_type == "DEMAND" and curr_close < zone.level_start:
                    self.swing_zones[idx] = SwingZone(
                        level_start=zone.level_start,
                        level_end=zone.level_end,
                        zone_type=zone.zone_type,
                        mitigated=True,
                        mitigation_index=self.global_index,
                        strength=zone.strength,
                    )

    def _update_sr_levels(self):
        """
        Calculate dynamic support and resistance levels from swing points.

        Support: Recent swing lows that price is above
        Resistance: Recent swing highs that price is below
        Deprecated/merged levels are filtered out.
        """
        if not self.candles:
            return

        curr_close = self.candles[-1]["close"]
        candle_slice = self.candles[-100:]

        # ── Support Levels (from swing lows) ─────────────────────
        supports: List[PriceLevel] = []
        if self.protected_low:
            supports.append(self.protected_low)

        for sw in reversed(self.swing_lows[-15:]):
            if curr_close > sw.price:
                # Count touches
                touches = 0
                sw_price = sw.price
                for c in candle_slice:
                    if abs(c["low"] - sw_price) / max(sw_price, 1.0) < 0.0015:
                        touches += 1

                strength = min(100.0, 50.0 + touches * 10 - (self.global_index - sw.index) * 0.05)
                supports.append(PriceLevel(
                    price=sw_price,
                    level_type="SUPPORT",
                    strength=max(10.0, strength),
                    touches=touches,
                    age=self.global_index - sw.index,
                    is_protected=False,
                ))

        # Merge nearby levels (within 0.2%)
        supports = sorted(supports, key=lambda x: x.price, reverse=True)
        filtered_supports: List[PriceLevel] = []
        for s in supports:
            if not any(abs(s.price - fs.price) / max(s.price, 1.0) < 0.002 for fs in filtered_supports):
                filtered_supports.append(s)
        self.support_levels = filtered_supports[:10]

        # ── Resistance Levels (from swing highs) ─────────────────
        resistances: List[PriceLevel] = []
        if self.protected_high:
            resistances.append(self.protected_high)

        for sw in reversed(self.swing_highs[-15:]):
            if curr_close < sw.price:
                touches = 0
                sw_price = sw.price
                for c in candle_slice:
                    if abs(c["high"] - sw_price) / max(sw_price, 1.0) < 0.0015:
                        touches += 1

                strength = min(100.0, 50.0 + touches * 10 - (self.global_index - sw.index) * 0.05)
                resistances.append(PriceLevel(
                    price=sw_price,
                    level_type="RESISTANCE",
                    strength=max(10.0, strength),
                    touches=touches,
                    age=self.global_index - sw.index,
                    is_protected=False,
                ))

        resistances = sorted(resistances, key=lambda x: x.price)
        filtered_resistances: List[PriceLevel] = []
        for r in resistances:
            if not any(abs(r.price - fr.price) / max(r.price, 1.0) < 0.002 for fr in filtered_resistances):
                filtered_resistances.append(r)
        self.resistance_levels = filtered_resistances[:10]

    # ==========================================================================
    # 7. LIQUIDITY ANALYSIS
    # ==========================================================================

    def _detect_liquidity(self):
        """
        Detect institutional liquidity patterns using only swing structure.

        Detects:
        - Equal Highs (EQH): Two or more swing highs at similar price
        - Equal Lows (EQL): Two or more swing lows at similar price
        - Liquidity Sweeps: Price exceeds a swing high/low then closes back
        - Swing Failure Pattern (SFP): Price exceeds swing level then closes below/above
        - False Breakouts / Breakout Traps: Recent break reverses
        """
        if len(self.candles) < 3:
            return

        curr_high = self.candles[-1]["high"]
        curr_low = self.candles[-1]["low"]
        curr_close = self.candles[-1]["close"]
        curr_volume = self.candles[-1]["volume"]

        # ── 1. Equal Highs (EQH) detection ───────────────────────
        if len(self.swing_highs) >= 2:
            recent_highs = self.swing_highs[-8:]
            for i in range(len(recent_highs)):
                for j in range(i + 1, len(recent_highs)):
                    h1, h2 = recent_highs[i], recent_highs[j]
                    if abs(h1.price - h2.price) / max(h1.price, 1.0) < 0.0015:
                        confidence = min(100.0, 60.0 + (h1.quality + h2.quality) / 2 * 0.3)
                        self._add_liquidity_event(
                            LiquidityType.EQH,
                            h2.price,
                            curr_volume,
                            70.0,
                            confidence,
                            f"Equal Highs at {h2.price:.2f} ({h1.index}, {h2.index})",
                        )

        # ── 2. Equal Lows (EQL) detection ────────────────────────
        if len(self.swing_lows) >= 2:
            recent_lows = self.swing_lows[-8:]
            for i in range(len(recent_lows)):
                for j in range(i + 1, len(recent_lows)):
                    l1, l2 = recent_lows[i], recent_lows[j]
                    if abs(l1.price - l2.price) / max(l1.price, 1.0) < 0.0015:
                        confidence = min(100.0, 60.0 + (l1.quality + l2.quality) / 2 * 0.3)
                        self._add_liquidity_event(
                            LiquidityType.EQL,
                            l2.price,
                            curr_volume,
                            70.0,
                            confidence,
                            f"Equal Lows at {l2.price:.2f} ({l1.index}, {l2.index})",
                        )

        # ── 3. Liquidity Sweeps & SFP ────────────────────────────
        last_maj_h = self._get_last_major_swing(SwingType.HIGH)
        last_maj_l = self._get_last_major_swing(SwingType.LOW)

        if last_maj_h:
            # Price broke above major swing high but closed at or below it
            if curr_high > last_maj_h.price and curr_close <= last_maj_h.price:
                # Check if it's an SFP (close below body of swing candle)
                swing_candle = self._get_candle_by_abs_idx(last_maj_h.index)
                if swing_candle:
                    swing_body_top = max(swing_candle["open"], swing_candle["close"])
                    if curr_close < swing_body_top:
                        self._add_liquidity_event(
                            LiquidityType.SFP_HIGH,
                            last_maj_h.price,
                            curr_volume,
                            85.0,
                            90.0,
                            f"SFP High at {last_maj_h.price:.2f} (liquidity grab + rejection)",
                        )
                    else:
                        self._add_liquidity_event(
                            LiquidityType.LIQUIDITY_SWEEP_HIGH,
                            last_maj_h.price,
                            curr_volume,
                            70.0,
                            75.0,
                            f"Liquidity Sweep above {last_maj_h.price:.2f}",
                        )

        if last_maj_l:
            # Price broke below major swing low but closed at or above it
            if curr_low < last_maj_l.price and curr_close >= last_maj_l.price:
                swing_candle = self._get_candle_by_abs_idx(last_maj_l.index)
                if swing_candle:
                    swing_body_bottom = min(swing_candle["open"], swing_candle["close"])
                    if curr_close > swing_body_bottom:
                        self._add_liquidity_event(
                            LiquidityType.SFP_LOW,
                            last_maj_l.price,
                            curr_volume,
                            85.0,
                            90.0,
                            f"SFP Low at {last_maj_l.price:.2f} (liquidity grab + rejection)",
                        )
                    else:
                        self._add_liquidity_event(
                            LiquidityType.LIQUIDITY_SWEEP_LOW,
                            last_maj_l.price,
                            curr_volume,
                            70.0,
                            75.0,
                            f"Liquidity Sweep below {last_maj_l.price:.2f}",
                        )

        # ── 4. False Breakout / Breakout Trap ────────────────────
        if self.recent_breaks:
            last_break = self.recent_breaks[-1]
            if (self.global_index - last_break.index) <= 3:
                if last_break.direction == MarketDirection.BULLISH and curr_close < last_break.level:
                    self._add_liquidity_event(
                        LiquidityType.FALSE_BREAKOUT,
                        last_break.level,
                        curr_volume,
                        80.0,
                        85.0,
                        f"Bullish False Breakout at {last_break.level:.2f}",
                    )
                elif last_break.direction == MarketDirection.BEARISH and curr_close > last_break.level:
                    self._add_liquidity_event(
                        LiquidityType.FALSE_BREAKOUT,
                        last_break.level,
                        curr_volume,
                        80.0,
                        85.0,
                        f"Bearish False Breakout at {last_break.level:.2f}",
                    )

        # ── 5. Buy-side / Sell-side Liquidity Labels ─────────────
        # Buy-side liquidity resides above swing highs (stop hunts)
        # Sell-side liquidity resides below swing lows
        if len(self.swing_highs) >= 2:
            recent_sh = self.swing_highs[-2:]
            if abs(recent_sh[0].price - recent_sh[1].price) / max(recent_sh[0].price, 1.0) < 0.003:
                # Tight cluster of highs = buy-side liquidity
                avg_price = (recent_sh[0].price + recent_sh[1].price) / 2
                self._add_liquidity_event(
                    LiquidityType.BUY_SIDE_LIQUIDITY,
                    avg_price,
                    curr_volume,
                    60.0,
                    65.0,
                    f"Buy-side Liquidity at {avg_price:.2f} (tight high cluster)",
                )

        if len(self.swing_lows) >= 2:
            recent_sl = self.swing_lows[-2:]
            if abs(recent_sl[0].price - recent_sl[1].price) / max(recent_sl[0].price, 1.0) < 0.003:
                avg_price = (recent_sl[0].price + recent_sl[1].price) / 2
                self._add_liquidity_event(
                    LiquidityType.SELL_SIDE_LIQUIDITY,
                    avg_price,
                    curr_volume,
                    60.0,
                    65.0,
                    f"Sell-side Liquidity at {avg_price:.2f} (tight low cluster)",
                )

    def _add_liquidity_event(
        self,
        ltype: LiquidityType,
        price: float,
        volume: float,
        quality: float,
        confidence: float,
        desc: str,
    ):
        """Add a liquidity event, deduplicating last event of same type."""
        if self.liquidity_events:
            last = self.liquidity_events[-1]
            if last.index == self.global_index and last.liquidity_type == ltype:
                return

        self.liquidity_events.append(LiquidityEvent(
            index=self.global_index,
            liquidity_type=ltype,
            price=price,
            volume=volume,
            quality=quality,
            confidence=confidence,
            description=desc,
        ))
        if len(self.liquidity_events) > 30:
            self.liquidity_events.pop(0)

    # ==========================================================================
    # 8. MARKET PATTERN RECOGNITION (Swing-Based)
    # ==========================================================================

    def _detect_price_patterns(self):
        """
        Detect chart patterns using verified swing points (not simple candle matching).

        Continuation: Bull/Bear Flag, Triangles, Channels, Rectangle
        Reversal: Double/Triple Top/Bottom, H&S, Wedges
        """
        swings = sorted(
            self.swing_highs + self.swing_lows,
            key=lambda x: x.index,
        )
        if len(swings) < 4:
            return

        curr_close = self.candles[-1]["close"]

        def approx_eq(p1: float, p2: float, tol: float = 0.002) -> bool:
            return abs(p1 - p2) / max(p1, 1.0) < tol

        # ── Reversal Patterns ────────────────────────────────────

        # Double Top / Bottom (requires at least 3 swings)
        if len(swings) >= 3:
            s0, s1, s2 = swings[-3:]

            # Double Top: High - Low - High with equal highs
            if (s0.swing_type == SwingType.HIGH
                    and s1.swing_type == SwingType.LOW
                    and s2.swing_type == SwingType.HIGH
                    and approx_eq(s0.price, s2.price)):

                state = PatternState.DEVELOPING
                if curr_close < s1.price:
                    state = PatternState.CONFIRMED
                elif curr_close > max(s0.price, s2.price):
                    state = PatternState.FAILED

                confidence = 75.0 if state == PatternState.CONFIRMED else 45.0
                quality = min(100.0, (s0.quality + s2.quality) / 2 + 10.0)

                self._add_pattern(PricePattern(
                    pattern_type=PatternType.DOUBLE_TOP,
                    direction=MarketDirection.BEARISH,
                    state=state,
                    confidence=confidence,
                    quality=quality,
                    breakout_level=s1.price,            # Neckline
                    invalidation_level=max(s0.price, s2.price),
                    age=self.global_index - s2.index,
                    neckline=s1.price,
                ))

            # Double Bottom: Low - High - Low with equal lows
            elif (s0.swing_type == SwingType.LOW
                  and s1.swing_type == SwingType.HIGH
                  and s2.swing_type == SwingType.LOW
                  and approx_eq(s0.price, s2.price)):

                state = PatternState.DEVELOPING
                if curr_close > s1.price:
                    state = PatternState.CONFIRMED
                elif curr_close < min(s0.price, s2.price):
                    state = PatternState.FAILED

                confidence = 75.0 if state == PatternState.CONFIRMED else 45.0
                quality = min(100.0, (s0.quality + s2.quality) / 2 + 10.0)

                self._add_pattern(PricePattern(
                    pattern_type=PatternType.DOUBLE_BOTTOM,
                    direction=MarketDirection.BULLISH,
                    state=state,
                    confidence=confidence,
                    quality=quality,
                    breakout_level=s1.price,
                    invalidation_level=min(s0.price, s2.price),
                    age=self.global_index - s2.index,
                    neckline=s1.price,
                ))

        # Triple Top / Bottom and H&S (requires at least 5 swings)
        if len(swings) >= 5:
            s0, s1, s2, s3, s4 = swings[-5:]

            # Head & Shoulders: High - Low - Higher High - Low - Lower High
            if (s0.swing_type == SwingType.HIGH
                    and s1.swing_type == SwingType.LOW
                    and s2.swing_type == SwingType.HIGH
                    and s3.swing_type == SwingType.LOW
                    and s4.swing_type == SwingType.HIGH):

                # Head is the highest middle high (s2)
                if s2.price > s0.price and s2.price > s4.price:
                    neckline = max(s1.price, s3.price)
                    state = PatternState.DEVELOPING
                    if curr_close < neckline:
                        state = PatternState.CONFIRMED
                    elif curr_close > s2.price:
                        state = PatternState.FAILED

                    confidence = 85.0 if state == PatternState.CONFIRMED else 55.0
                    quality = (s1.quality + s3.quality) / 2

                    self._add_pattern(PricePattern(
                        pattern_type=PatternType.HEAD_AND_SHOULDERS,
                        direction=MarketDirection.BEARISH,
                        state=state,
                        confidence=confidence,
                        quality=quality,
                        breakout_level=neckline,
                        invalidation_level=s2.price,
                        age=self.global_index - s4.index,
                        neckline=neckline,
                    ))

                # Triple Top: Three equal highs
                elif (approx_eq(s0.price, s2.price) and approx_eq(s2.price, s4.price)):
                    neckline = min(s1.price, s3.price)
                    state = PatternState.DEVELOPING
                    if curr_close < neckline:
                        state = PatternState.CONFIRMED
                    elif curr_close > max(s0.price, s2.price, s4.price):
                        state = PatternState.FAILED

                    confidence = 80.0 if state == PatternState.CONFIRMED else 50.0
                    quality = (s1.quality + s3.quality) / 2

                    self._add_pattern(PricePattern(
                        pattern_type=PatternType.TRIPLE_TOP,
                        direction=MarketDirection.BEARISH,
                        state=state,
                        confidence=confidence,
                        quality=quality,
                        breakout_level=neckline,
                        invalidation_level=max(s0.price, s2.price, s4.price),
                        age=self.global_index - s4.index,
                        neckline=neckline,
                    ))

            # Inverse Head & Shoulders
            elif (s0.swing_type == SwingType.LOW
                  and s1.swing_type == SwingType.HIGH
                  and s2.swing_type == SwingType.LOW
                  and s3.swing_type == SwingType.HIGH
                  and s4.swing_type == SwingType.LOW):

                if s2.price < s0.price and s2.price < s4.price:
                    neckline = min(s1.price, s3.price)
                    state = PatternState.DEVELOPING
                    if curr_close > neckline:
                        state = PatternState.CONFIRMED
                    elif curr_close < s2.price:
                        state = PatternState.FAILED

                    confidence = 85.0 if state == PatternState.CONFIRMED else 55.0
                    quality = (s1.quality + s3.quality) / 2

                    self._add_pattern(PricePattern(
                        pattern_type=PatternType.INVERSE_HEAD_AND_SHOULDERS,
                        direction=MarketDirection.BULLISH,
                        state=state,
                        confidence=confidence,
                        quality=quality,
                        breakout_level=neckline,
                        invalidation_level=s2.price,
                        age=self.global_index - s4.index,
                        neckline=neckline,
                    ))

                # Triple Bottom: Three equal lows
                elif (approx_eq(s0.price, s2.price) and approx_eq(s2.price, s4.price)):
                    neckline = max(s1.price, s3.price)
                    state = PatternState.DEVELOPING
                    if curr_close > neckline:
                        state = PatternState.CONFIRMED
                    elif curr_close < min(s0.price, s2.price, s4.price):
                        state = PatternState.FAILED

                    confidence = 80.0 if state == PatternState.CONFIRMED else 50.0
                    quality = (s1.quality + s3.quality) / 2

                    self._add_pattern(PricePattern(
                        pattern_type=PatternType.TRIPLE_BOTTOM,
                        direction=MarketDirection.BULLISH,
                        state=state,
                        confidence=confidence,
                        quality=quality,
                        breakout_level=neckline,
                        invalidation_level=min(s0.price, s2.price, s4.price),
                        age=self.global_index - s4.index,
                        neckline=neckline,
                    ))

        # ── Continuation Patterns (Triangles, Channels, Wedges) ──
        high_swings = [s for s in swings if s.swing_type == SwingType.HIGH]
        low_swings = [s for s in swings if s.swing_type == SwingType.LOW]

        if len(high_swings) >= 3 and len(low_swings) >= 3:
            h3 = high_swings[-3:]
            l3 = low_swings[-3:]

            h_prices = [h.price for h in h3]
            h_indices = [h.index for h in h3]
            l_prices = [l.price for l in l3]
            l_indices = [l.index for l in l3]

            # Calculate slopes
            h_slope = 0.0
            if h_indices[-1] - h_indices[0] > 0:
                h_slope = ((h_prices[-1] - h_prices[0]) / max(h_prices[0], 1.0)) / (h_indices[-1] - h_indices[0])

            l_slope = 0.0
            if l_indices[-1] - l_indices[0] > 0:
                l_slope = ((l_prices[-1] - l_prices[0]) / max(l_prices[0], 1.0)) / (l_indices[-1] - l_indices[0])

            h_slope_norm = h_slope * 1000.0  # Scale for readability
            l_slope_norm = l_slope * 1000.0

            # Symmetrical Triangle: converging highs and lows
            if h_slope_norm < -0.1 and l_slope_norm > 0.1:
                state = PatternState.DEVELOPING
                if curr_close > h_prices[-1]:
                    state = PatternState.CONFIRMED
                    direction = MarketDirection.BULLISH
                elif curr_close < l_prices[-1]:
                    state = PatternState.CONFIRMED
                    direction = MarketDirection.BEARISH
                else:
                    direction = MarketDirection.BULLISH  # default upside bias

                self._add_pattern(PricePattern(
                    pattern_type=PatternType.SYMMETRICAL_TRIANGLE,
                    direction=direction,
                    state=state,
                    confidence=60.0,
                    quality=70.0,
                    breakout_level=h_prices[-1],
                    invalidation_level=l_prices[-1],
                    age=self.global_index - high_swings[-1].index,
                ))

            # Ascending Triangle: flat highs, rising lows
            elif abs(h_slope_norm) < 0.1 and l_slope_norm > 0.1:
                state = PatternState.DEVELOPING
                if curr_close > h_prices[-1]:
                    state = PatternState.CONFIRMED

                self._add_pattern(PricePattern(
                    pattern_type=PatternType.ASCENDING_TRIANGLE,
                    direction=MarketDirection.BULLISH,
                    state=state,
                    confidence=65.0,
                    quality=75.0,
                    breakout_level=h_prices[-1],
                    invalidation_level=l_prices[-1],
                    age=self.global_index - high_swings[-1].index,
                ))

            # Descending Triangle: falling highs, flat lows
            elif h_slope_norm < -0.1 and abs(l_slope_norm) < 0.1:
                state = PatternState.DEVELOPING
                if curr_close < l_prices[-1]:
                    state = PatternState.CONFIRMED

                self._add_pattern(PricePattern(
                    pattern_type=PatternType.DESCENDING_TRIANGLE,
                    direction=MarketDirection.BEARISH,
                    state=state,
                    confidence=65.0,
                    quality=75.0,
                    breakout_level=l_prices[-1],
                    invalidation_level=h_prices[-1],
                    age=self.global_index - low_swings[-1].index,
                ))

            # Falling Wedge: falling highs and lows, but highs falling faster
            elif h_slope_norm < -0.1 and l_slope_norm < -0.1 and h_slope_norm < l_slope_norm:
                state = PatternState.DEVELOPING
                if curr_close > h_prices[-1]:
                    state = PatternState.CONFIRMED

                self._add_pattern(PricePattern(
                    pattern_type=PatternType.FALLING_WEDGE,
                    direction=MarketDirection.BULLISH,
                    state=state,
                    confidence=70.0,
                    quality=80.0,
                    breakout_level=h_prices[-1],
                    invalidation_level=l_prices[-1],
                    age=self.global_index - high_swings[-1].index,
                ))

            # Rising Wedge: rising highs and lows, but lows rising faster
            elif h_slope_norm > 0.1 and l_slope_norm > 0.1 and l_slope_norm > h_slope_norm:
                state = PatternState.DEVELOPING
                if curr_close < l_prices[-1]:
                    state = PatternState.CONFIRMED

                self._add_pattern(PricePattern(
                    pattern_type=PatternType.RISING_WEDGE,
                    direction=MarketDirection.BEARISH,
                    state=state,
                    confidence=70.0,
                    quality=80.0,
                    breakout_level=l_prices[-1],
                    invalidation_level=h_prices[-1],
                    age=self.global_index - low_swings[-1].index,
                ))

            # Rising Channel: both rising at similar rate
            elif h_slope_norm > 0.05 and l_slope_norm > 0.05 and abs(h_slope_norm - l_slope_norm) < 0.15:
                self._add_pattern(PricePattern(
                    pattern_type=PatternType.RISING_CHANNEL,
                    direction=MarketDirection.BULLISH,
                    state=PatternState.DEVELOPING,
                    confidence=65.0,
                    quality=75.0,
                    breakout_level=h_prices[-1],
                    invalidation_level=l_prices[-1],
                    age=self.global_index - high_swings[-1].index,
                ))

            # Falling Channel: both falling at similar rate
            elif h_slope_norm < -0.05 and l_slope_norm < -0.05 and abs(h_slope_norm - l_slope_norm) < 0.15:
                self._add_pattern(PricePattern(
                    pattern_type=PatternType.FALLING_CHANNEL,
                    direction=MarketDirection.BEARISH,
                    state=PatternState.DEVELOPING,
                    confidence=65.0,
                    quality=75.0,
                    breakout_level=h_prices[-1],
                    invalidation_level=l_prices[-1],
                    age=self.global_index - high_swings[-1].index,
                ))

            # Rectangle / Range: flat highs AND flat lows
            if abs(h_slope_norm) < 0.1 and abs(l_slope_norm) < 0.1:
                # Check range width is tight (less than 3%)
                range_width = (max(h_prices) - min(l_prices)) / max(min(l_prices), 1.0)
                if range_width < 0.03:
                    self._add_pattern(PricePattern(
                        pattern_type=PatternType.RECTANGLE,
                        direction=MarketDirection.UNDEFINED,
                        state=PatternState.DEVELOPING,
                        confidence=60.0,
                        quality=70.0,
                        breakout_level=max(h_prices),
                        invalidation_level=min(l_prices),
                        age=self.global_index - high_swings[-1].index,
                    ))

        # ── Flag Patterns (using impulse + consolidation) ────────
        if len(self.candles) >= 20:
            # Bull Flag: sharp rise (flagpole) + tight consolidation
            pole_candles = self.candles[-15:-5]
            low_pole = min(c["low"] for c in pole_candles)
            high_pole = max(c["high"] for c in pole_candles)
            pole_height = high_pole - low_pole

            if pole_height > 0 and (pole_candles[-1]["close"] - pole_candles[0]["open"]) / max(pole_candles[0]["open"], 1.0) > 0.025:
                cons_candles = self.candles[-5:]
                cons_high = max(c["high"] for c in cons_candles)
                cons_low = min(c["low"] for c in cons_candles)

                if (cons_high - cons_low) < (pole_height * 0.4):
                    state = PatternState.DEVELOPING
                    if curr_close > cons_high:
                        state = PatternState.CONFIRMED

                    self._add_pattern(PricePattern(
                        pattern_type=PatternType.BULL_FLAG,
                        direction=MarketDirection.BULLISH,
                        state=state,
                        confidence=70.0,
                        quality=75.0,
                        breakout_level=cons_high,
                        invalidation_level=cons_low,
                        age=5,
                    ))

            # Bear Flag: sharp drop (flagpole) + tight consolidation
            if pole_height > 0 and (pole_candles[0]["open"] - pole_candles[-1]["close"]) / max(pole_candles[0]["open"], 1.0) > 0.025:
                cons_candles = self.candles[-5:]
                cons_high = max(c["high"] for c in cons_candles)
                cons_low = min(c["low"] for c in cons_candles)

                if (cons_high - cons_low) < (pole_height * 0.4):
                    state = PatternState.DEVELOPING
                    if curr_close < cons_low:
                        state = PatternState.CONFIRMED

                    self._add_pattern(PricePattern(
                        pattern_type=PatternType.BEAR_FLAG,
                        direction=MarketDirection.BEARISH,
                        state=state,
                        confidence=70.0,
                        quality=75.0,
                        breakout_level=cons_low,
                        invalidation_level=cons_high,
                        age=5,
                    ))

    def _add_pattern(self, pattern: PricePattern):
        """Register a detected pattern, updating if same type exists."""
        for idx, p in enumerate(self.patterns):
            if p.pattern_type == pattern.pattern_type:
                self.patterns[idx] = pattern
                return
        self.patterns.append(pattern)
        if len(self.patterns) > 20:
            self.patterns.pop(0)

    # ==========================================================================
    # 9. MARKET PHASE CLASSIFICATION
    # ==========================================================================

    def _determine_market_phase(self) -> MarketPhaseResult:
        """
        Classify the current market phase based on structural analysis.

        Phases:
        - STRONG_TREND: Multiple HH/HL or LH/LL with quality swings
        - WEAK_TREND: Single HH/HL or LH/LL, low quality
        - PULLBACK: Counter-trend move within existing structure
        - BREAKOUT: Recent BOS/CHOCH
        - COMPRESSION: Tight range, forming triangle/rectangle
        - EXPANSION: Unusually large candle range + volume
        - ACCUMULATION: Range with bullish bias (higher lows)
        - DISTRIBUTION: Range with bearish bias (lower highs)
        - RANGE: Clear sideways with no directional bias
        - TRANSITION: Recent CHOCH (change of character)
        """
        if not self.candles:
            return MarketPhaseResult(
                phase=MarketPhase.RANGE,
                clarity=50.0,
                description="No data available",
            )

        curr_close = self.candles[-1]["close"]
        curr_open = self.candles[-1]["open"]
        trend = self._get_trend_metrics()

        # ── Check for recent CHOCH (Transition Phase) ────────────
        for brk in reversed(self.recent_breaks):
            if brk.break_type == BreakType.CHOCH:
                if (self.global_index - brk.index) <= 5:
                    return MarketPhaseResult(
                        phase=MarketPhase.TRANSITION,
                        clarity=75.0,
                        description=f"Recent {brk.direction.value} CHOCH at {brk.level:.2f}",
                    )

        # ── Check for recent Breakout ─────────────────────────────
        for brk in reversed(self.recent_breaks):
            if brk.break_type == BreakType.BOS and brk.status == BreakoutStatus.CONFIRMED:
                if (self.global_index - brk.index) <= 2:
                    return MarketPhaseResult(
                        phase=MarketPhase.BREAKOUT,
                        clarity=80.0,
                        description=f"Bullish BOS" if brk.direction == MarketDirection.BULLISH else f"Bearish BOS",
                    )

        # ── Check for Compression (triangle/wedge/rectangle patterns) ──
        active_pattern = None
        for p in self.patterns[-3:]:
            if p.state == PatternState.DEVELOPING and p.pattern_type in [
                PatternType.SYMMETRICAL_TRIANGLE,
                PatternType.ASCENDING_TRIANGLE,
                PatternType.DESCENDING_TRIANGLE,
                PatternType.RISING_WEDGE,
                PatternType.FALLING_WEDGE,
                PatternType.RECTANGLE,
            ]:
                active_pattern = p
                break

        if active_pattern is not None:
            return MarketPhaseResult(
                phase=MarketPhase.COMPRESSION,
                clarity=70.0,
                description=f"{active_pattern.pattern_type.value} forming",
            )

        # ── Check for Expansion (large candle + volume spike) ─────
        if len(self.candles) >= 10:
            last_10 = self.candles[-10:]
            avg_body = sum(abs(c["close"] - c["open"]) for c in last_10) / 10.0
            avg_vol = sum(c["volume"] for c in last_10) / 10.0
            curr_body = abs(curr_close - curr_open)
            if curr_body > (avg_body * 2.0) and self.candles[-1]["volume"] > (avg_vol * 1.8):
                return MarketPhaseResult(
                    phase=MarketPhase.EXPANSION,
                    clarity=75.0,
                    description=f"Expansion candle: body {curr_body:.2f} vs avg {avg_body:.2f}",
                )

        # ── Strong Trend Phase ────────────────────────────────────
        if trend.direction in (TrendDirection.UPTREND, TrendDirection.DOWNTREND):
            if trend.impulse_count >= 3 and trend.strength > 60:
                return MarketPhaseResult(
                    phase=MarketPhase.STRONG_TREND,
                    clarity=85.0,
                    description=f"{trend.direction.value} with {trend.impulse_count} impulses",
                )
            else:
                return MarketPhaseResult(
                    phase=MarketPhase.WEAK_TREND,
                    clarity=65.0,
                    description=f"Early {trend.direction.value} developing",
                )

        # ── Accumulation / Distribution: Range with bias ──────────
        if trend.direction == TrendDirection.SIDEWAYS and len(self.candles) >= 30:
            prices = [c["close"] for c in self.candles[-30:]]
            max_p, min_p = max(prices), min(prices)
            range_pct = (max_p - min_p) / max(min_p, 1.0)

            if range_pct < 0.025:
                # Check bias within the range
                if len(self.swing_lows) >= 2:
                    last_two_lows = self.swing_lows[-2:]
                    if last_two_lows[1].price > last_two_lows[0].price:
                        return MarketPhaseResult(
                            phase=MarketPhase.ACCUMULATION,
                            clarity=60.0,
                            description="Range with rising lows (accumulation)",
                        )

                if len(self.swing_highs) >= 2:
                    last_two_highs = self.swing_highs[-2:]
                    if last_two_highs[1].price < last_two_highs[0].price:
                        return MarketPhaseResult(
                            phase=MarketPhase.DISTRIBUTION,
                            clarity=60.0,
                            description="Range with falling highs (distribution)",
                        )

                # Pure range (no bias)
                return MarketPhaseResult(
                    phase=MarketPhase.RANGE,
                    clarity=70.0,
                    description=f"Sideways range ({range_pct * 100:.2f}%)",
                )

        # ── Default: Range ────────────────────────────────────────
        return MarketPhaseResult(
            phase=MarketPhase.RANGE,
            clarity=50.0,
            description="No clear market phase detected",
        )

    # ==========================================================================
    # 10. CONFIDENCE ENGINE (0-100)
    # ==========================================================================

    def _compute_confidence_score(self, phase: MarketPhase) -> float:
        """
        Compute overall structural confidence score using ONLY:
        - Swing quality
        - Trend quality
        - BOS/CHOCH quality
        - Pattern quality
        - Liquidity signals
        - Support/Resistance proximity
        - Market phase clarity

        No technical indicators used.
        """
        scores = []

        # 1. Swing Quality (weight: 0.20)
        all_swings = self.swing_highs[-3:] + self.swing_lows[-3:]
        if all_swings:
            avg_quality = sum(s.quality for s in all_swings) / len(all_swings)
            scores.append((avg_quality, 0.20))
        else:
            scores.append((50.0, 0.20))

        # 2. Trend Quality (weight: 0.20)
        trend = self._get_trend_metrics()
        trend_score = 50.0
        if trend.direction in (TrendDirection.UPTREND, TrendDirection.DOWNTREND):
            trend_score = (trend.strength * 0.5 + trend.quality * 0.5)
            # Penalize very old trends
            if trend.age > 40:
                trend_score = max(40.0, trend_score - (trend.age - 40) * 0.3)
        scores.append((trend_score, 0.20))

        # 3. BOS/CHOCH Quality (weight: 0.15)
        bos_score = 50.0
        if self.recent_breaks:
            recent = self.recent_breaks[-3:]
            recent_confirmed = [b for b in recent if b.status == BreakoutStatus.CONFIRMED]
            if recent_confirmed:
                bos_score = sum(b.confidence for b in recent_confirmed) / len(recent_confirmed)
            elif [b for b in recent if b.status == BreakoutStatus.FAILED]:
                bos_score = 30.0  # Failed breaks reduce confidence
        scores.append((bos_score, 0.15))

        # 4. Pattern Quality (weight: 0.10)
        pattern_score = 50.0
        if self.patterns:
            recent_pattern = self.patterns[-1]
            if recent_pattern.state == PatternState.CONFIRMED:
                pattern_score = 90.0
            elif recent_pattern.state == PatternState.DEVELOPING:
                pattern_score = 65.0
            elif recent_pattern.state == PatternState.FAILED:
                pattern_score = 30.0
        scores.append((pattern_score, 0.10))

        # 5. Liquidity Quality (weight: 0.10)
        liquidity_score = 50.0
        if self.liquidity_events:
            recent_liquidity = self.liquidity_events[-3:]
            if recent_liquidity:
                avg_quality = sum(l.quality for l in recent_liquidity) / len(recent_liquidity)
                liquidity_score = avg_quality
        scores.append((liquidity_score, 0.10))

        # 6. Support/Resistance Proximity (weight: 0.10)
        sr_score = 50.0
        if self.support_levels or self.resistance_levels:
            curr_close = self.candles[-1]["close"] if self.candles else 0

            # Check proximity to nearest strong S/R
            nearest_support = None
            nearest_resistance = None

            if self.support_levels:
                above_supports = [s for s in self.support_levels if s.price < curr_close]
                if above_supports:
                    nearest_support = max(above_supports, key=lambda x: x.price)

            if self.resistance_levels:
                below_resistances = [r for r in self.resistance_levels if r.price > curr_close]
                if below_resistances:
                    nearest_resistance = min(below_resistances, key=lambda x: x.price)

            if nearest_support and nearest_resistance:
                # Price is in a defined range = good structure
                sr_range = (nearest_resistance.price - nearest_support.price) / max(curr_close, 1.0)
                if sr_range < 0.01:
                    sr_score = 80.0  # Tight range
                elif sr_range < 0.025:
                    sr_score = 70.0  # Clean range
                else:
                    sr_score = 60.0
            elif nearest_support:
                sr_score = 55.0
            elif nearest_resistance:
                sr_score = 55.0

        scores.append((sr_score, 0.10))

        # 7. Market Phase Clarity (weight: 0.15)
        phase_score = 50.0
        if phase in (MarketPhase.STRONG_TREND, MarketPhase.BREAKOUT, MarketPhase.EXPANSION):
            phase_score = 90.0
        elif phase in (MarketPhase.WEAK_TREND, MarketPhase.PULLBACK, MarketPhase.COMPRESSION):
            phase_score = 75.0
        elif phase == MarketPhase.TRANSITION:
            phase_score = 60.0
        elif phase in (MarketPhase.ACCUMULATION, MarketPhase.DISTRIBUTION):
            phase_score = 55.0
        elif phase == MarketPhase.RANGE:
            phase_score = 45.0
        scores.append((phase_score, 0.15))

        # Weighted calculation
        total_weight = sum(w for _, w in scores)
        if total_weight > 0:
            final_score = sum(val * w for val, w in scores) / total_weight
        else:
            final_score = 50.0

        return max(0.0, min(100.0, final_score))

    # ==========================================================================
    # RESULT BUILDER
    # ==========================================================================

    def _build_result(
        self,
        trend: TrendMetrics,
        phase_result: MarketPhaseResult,
        confidence: float,
    ) -> UnderlyingStructureResult:
        """Build the final UnderlyingStructureResult from current state."""

        # Major swing high/low
        major_high = self._get_last_major_swing(SwingType.HIGH)
        major_low = self._get_last_major_swing(SwingType.LOW)

        # Nearest support and resistance
        curr_close = self.candles[-1]["close"] if self.candles else 0.0
        nearest_support = None
        nearest_resistance = None

        if self.support_levels:
            above_supports = [s for s in self.support_levels if s.price < curr_close]
            if above_supports:
                nearest_support = max(above_supports, key=lambda x: x.price)

        if self.resistance_levels:
            below_resistances = [r for r in self.resistance_levels if r.price > curr_close]
            if below_resistances:
                nearest_resistance = min(below_resistances, key=lambda x: x.price)

        # Last BOS and CHOCH
        last_bos = None
        last_choch = None
        for brk in reversed(self.recent_breaks):
            if brk.break_type == BreakType.BOS and last_bos is None:
                last_bos = brk
            if brk.break_type == BreakType.CHOCH and last_choch is None:
                last_choch = brk
            if last_bos is not None and last_choch is not None:
                break

        # Active pattern
        active_pattern = None
        pattern_state = None
        pattern_confidence = None
        if self.patterns:
            # Find the most recent non-failed pattern
            for p in reversed(self.patterns):
                if p.state in (PatternState.DEVELOPING, PatternState.CONFIRMED):
                    active_pattern = p
                    pattern_state = p.state
                    pattern_confidence = p.confidence
                    break

        # Equal highs / lows (most recent)
        equal_high = None
        equal_low = None
        for evt in reversed(self.liquidity_events):
            if evt.liquidity_type == LiquidityType.EQH and equal_high is None:
                equal_high = evt.price
            if evt.liquidity_type == LiquidityType.EQL and equal_low is None:
                equal_low = evt.price
            if equal_high is not None and equal_low is not None:
                break

        # Last liquidity event
        last_liquidity = self.liquidity_events[-1] if self.liquidity_events else None

        # Debug information
        debug = {
            "candles_processed": self.global_index + 1,
            "swing_highs_count": len(self.swing_highs),
            "swing_lows_count": len(self.swing_lows),
            "breaks_count": len(self.recent_breaks),
            "patterns_detected": len(self.patterns),
            "liquidity_events": len(self.liquidity_events),
            "structure_sequence": self.structure_sequence[-10:],
            "consecutive_bullish": self.consecutive_bullish_swings,
            "consecutive_bearish": self.consecutive_bearish_swings,
            "trend_age": self.trend_age,
            "impulse_leg_slopes": self.impulse_leg_slopes[-5:] if self.impulse_leg_slopes else [],
            "support_count": len(self.support_levels),
            "resistance_count": len(self.resistance_levels),
            "protected_low_price": self.protected_low.price if self.protected_low else None,
            "protected_high_price": self.protected_high.price if self.protected_high else None,
        }

        return UnderlyingStructureResult(
            trend=trend.direction,
            trend_strength=trend.strength,
            trend_quality=trend.quality,
            trend_age=trend.age,
            market_bias=trend.market_bias,
            market_phase=phase_result.phase,
            confidence=confidence,
            major_swing_high=major_high.price if major_high else None,
            major_swing_low=major_low.price if major_low else None,
            support=nearest_support.price if nearest_support else None,
            resistance=nearest_resistance.price if nearest_resistance else None,
            protected_high=self.protected_high.price if self.protected_high else None,
            protected_low=self.protected_low.price if self.protected_low else None,
            last_bos=last_bos,
            last_choch=last_choch,
            active_pattern=active_pattern,
            pattern_state=pattern_state,
            pattern_confidence=pattern_confidence,
            equal_high=equal_high,
            equal_low=equal_low,
            liquidity_event=last_liquidity,
            debug_information=debug,
        )

    def _empty_result(self) -> UnderlyingStructureResult:
        """Return an empty/default result when no data is available."""
        return UnderlyingStructureResult(
            trend=TrendDirection.SIDEWAYS,
            trend_strength=0.0,
            trend_quality=0.0,
            trend_age=0,
            market_bias=MarketDirection.UNDEFINED,
            market_phase=MarketPhase.RANGE,
            confidence=50.0,
            major_swing_high=None,
            major_swing_low=None,
            support=None,
            resistance=None,
            protected_high=None,
            protected_low=None,
            last_bos=None,
            last_choch=None,
            active_pattern=None,
            pattern_state=None,
            pattern_confidence=None,
            equal_high=None,
            equal_low=None,
            liquidity_event=None,
            debug_information={},
        )


# ==============================================================================
# PUBLIC API
# ==============================================================================

def analyze(df: pd.DataFrame) -> UnderlyingStructureResult:
    """
    Analyze 5-minute underlying index OHLCV data and return the complete
    market structure analysis.

    Args:
        df: DataFrame with columns: open, high, low, close, volume
            (index can be any type; must have at least these columns)

    Returns:
        UnderlyingStructureResult with trend, structure, patterns, and confidence.

    This engine does NOT use: EMA, VWAP, ATR, ADX, RSI, MACD, Option Chain, OI.
    This engine does NOT generate BUY/SELL/CALL/PUT signals.
    """
    engine = UnderlyingMarketStructureEngine()

    n = len(df)
    if n == 0:
        return engine._empty_result()

    # Extract numpy arrays for speed
    opens = df["open"].to_numpy(dtype=float)
    highs = df["high"].to_numpy(dtype=float)
    lows = df["low"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    volumes = df["volume"].to_numpy(dtype=float) if "volume" in df.columns else np.zeros(n)
    times = df.index.to_numpy()

    # Process intermediate candles in fast mode
    for i in range(n - 1):
        candle = {
            "open": opens[i],
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "volume": volumes[i],
            "time": times[i],
        }
        engine.update(candle, full_analysis=False)

    # Full analysis on the last candle
    last_candle = {
        "open": opens[-1],
        "high": highs[-1],
        "low": lows[-1],
        "close": closes[-1],
        "volume": volumes[-1],
        "time": times[-1],
    }
    return engine.update(last_candle, full_analysis=True)

