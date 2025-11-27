# pymodbus >= 3.1.0
# Serve Modbus holding registers from a CSV.
# Devices 1..105: HR1 = R (Ω×10000), HR2 = U (V×1000), HR3 = T (°C×100)
# Device 106:     HR1 = I (A×100, signed 16-bit two's complement)
#
# CSV requirements:
#   - Columns: time (seconds, optional), I, and per-cell R1..R105, U1..U105, T1..T105
#   - If some per-cell columns are missing, they will be filled by cycling the
#     first 5 cells' series (1→6→11..., 2→7→12..., etc.) as a fallback.
#
# Windows: PORT = "COM5"
# Linux:   PORT = "/dev/ttyUSB0" or pseudo TTY "/dev/pts/8" or "/dev/ttyV1"

import os
import time
import threading
import datetime
import re
from typing import Dict, List, Tuple, Optional

import pandas as pd
from pymodbus.server import StartSerialServer
from pymodbus.datastore import (
    ModbusServerContext, ModbusDeviceContext,
    ModbusSequentialDataBlock, ModbusSparseDataBlock,
)

# ---------- CONFIG ----------
CSV_PATH = "data_5.csv"   # set to your new CSV file
PORT     = "/dev/ttyS2"                  # e.g. "COM5" on Windows
BAUD     = 9600
LOOP_CSV = True
USE_TIME_COLUMN = True
R_DEFAULT = 0.005  # Ω, used if no R data is available
MAX_DEVICES = 205 # number of cell devices
CURRENT_DEV_ID = 206
# ----------------------------

# ---------- Scaling ----------
R_SCALE = 10000   # Ω -> Ω×10000
V_SCALE = 1000    # V -> V×1000
T_SCALE = 100     # °C -> °C×100
I_SCALE = 100     # A -> A×100 (signed)
# ----------------------------

def enc_u16(val: float, scale: float) -> int:
    x = int(round(float(val) * scale))
    if x < 0: x = 0
    if x > 0xFFFF: x = 0xFFFF
    return x

def enc_s16(val: float, scale: float) -> int:
    x = int(round(float(val) * scale))
    return (x + (1 << 16)) % (1 << 16)

# ---------- Register blocks ----------
class MirrorHR(ModbusSparseDataBlock):
    # HR1=R, HR2=U, HR3=T
    def __init__(self):
        super().__init__({1: 0, 2: 0, 3: 0})
    def set3(self, r_int: int, v_int: int, t_int: int):
        self.values[1] = r_int
        self.values[2] = v_int
        self.values[3] = t_int

class CurrentHR(ModbusSparseDataBlock):
    # HR1=I
    def __init__(self):
        super().__init__({1: 0})
    def setI(self, i_int: int):
        self.values[1] = i_int

# ---------- Build devices ----------
devices: Dict[int, ModbusDeviceContext] = {}
hr_blocks: List[MirrorHR] = []

for dev_id in range(1, MAX_DEVICES + 1):
    b = MirrorHR()
    hr_blocks.append(b)
    devices[dev_id] = ModbusDeviceContext(
        di=ModbusSequentialDataBlock(1, [0] * 8),
        co=ModbusSequentialDataBlock(1, [0] * 8),
        hr=b,
        ir=ModbusSequentialDataBlock(1, [0] * 8),
    )

current_block = CurrentHR()
devices[CURRENT_DEV_ID] = ModbusDeviceContext(
    di=ModbusSequentialDataBlock(1, [0] * 8),
    co=ModbusSequentialDataBlock(1, [0] * 8),
    hr=current_block,
    ir=ModbusSequentialDataBlock(1, [0] * 8),
)

context = ModbusServerContext(devices=devices, single=False)

# ---------- CSV loader ----------
def load_csv(path: str) -> Tuple[Optional[pd.Series], pd.Series,
                                 Dict[int, pd.Series], Dict[int, pd.Series], Dict[int, pd.Series]]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")

    df = pd.read_csv(path)
    cols_lower = {c.lower(): c for c in df.columns}

    def get_col(name: str) -> Optional[pd.Series]:
        key = name.lower()
        if key in cols_lower:
            return df[cols_lower[key]].astype(float)
        return None

    t = get_col("time")
    I = get_col("I")
    if I is None:
        raise ValueError("CSV must contain column 'I' (current, A).")

    # Collect per-device columns R1..R105 etc.
    Rdev: Dict[int, pd.Series] = {}
    Udev: Dict[int, pd.Series] = {}
    Tdev: Dict[int, pd.Series] = {}

    # Determine per-cell columns using both styles: R1 / R_1
    def pick(metric: str, idx: int) -> Optional[pd.Series]:
        c1 = get_col(f"{metric}{idx}")
        if c1 is not None:
            return c1
        c2 = get_col(f"{metric}_{idx}")
        return c2

    # Read available columns
    for d in range(1, MAX_DEVICES + 1):
        r = pick("R", d)
        u = pick("U", d)
        tt = pick("T", d)
        if r is not None and u is not None and tt is not None:
            Rdev[d] = r
            Udev[d] = u
            Tdev[d] = tt

    # Fallback if not all 1..105 present: cycle the first 5 cells (common in your pipeline)
    if len(Rdev) < MAX_DEVICES or len(Udev) < MAX_DEVICES or len(Tdev) < MAX_DEVICES:
        # Try to grab 1..5 first; if R1..R5 missing, try singles or defaults
        base = range(1, 6)
        have_any = all(pick("U", i) is not None and pick("T", i) is not None for i in base)
        if have_any:
            R_single = get_col("R") or get_col("R_OHM") or get_col("R0_EST_OHM")
            if R_single is None and all(pick("R", i) is None for i in base):
                # build a constant series if R is truly missing in base columns
                R_single = pd.Series([R_DEFAULT] * len(I), dtype=float)
            # Build base arrays
            Rb = [pick("R", i) if pick("R", i) is not None else R_single for i in base]
            Ub = [pick("U", i) for i in base]
            Tb = [pick("T", i) for i in base]
            # Trim to common n
            n = len(I)
            for k in range(5):
                if Rb[k] is not None: n = min(n, len(Rb[k]))
                n = min(n, len(Ub[k]), len(Tb[k]))
            I = I.iloc[:n]
            if t is not None: t = t.iloc[:n]
            for k in range(5):
                if Rb[k] is None:
                    Rb[k] = pd.Series([R_DEFAULT] * n, dtype=float)
                else:
                    Rb[k] = Rb[k].iloc[:n]
                Ub[k] = Ub[k].iloc[:n]
                Tb[k] = Tb[k].iloc[:n]
            # Fill 1..105 by cycling base 1..5
            for d in range(1, MAX_DEVICES + 1):
                k = (d - 1) % 5  # 0..4
                Rdev[d] = Rb[k]
                Udev[d] = Ub[k]
                Tdev[d] = Tb[k]
        else:
            # Last-resort: mirror single columns to all devices
            R_single = get_col("R") or get_col("R_OHM") or get_col("R0_EST_OHM")
            U_single = get_col("U") or get_col("VOLTAGE") or get_col("U_V")
            T_single = get_col("T") or get_col("T_C") or get_col("TEMPERATURE") or get_col("TEMP")
            if U_single is None or T_single is None:
                raise ValueError("CSV must have per-cell U/T or single U/T columns.")
            if R_single is None:
                R_single = pd.Series([R_DEFAULT] * len(U_single), dtype=float)
            n = min(len(I), len(U_single), len(T_single), len(R_single))
            I = I.iloc[:n]
            if t is not None: t = t.iloc[:n]
            R_single = R_single.iloc[:n]
            U_single = U_single.iloc[:n]
            T_single = T_single.iloc[:n]
            for d in range(1, MAX_DEVICES + 1):
                Rdev[d] = R_single
                Udev[d] = U_single
                Tdev[d] = T_single
    else:
        # All per-cell provided → align lengths
        n = len(I)
        for d in range(1, MAX_DEVICES + 1):
            n = min(n, len(Rdev[d]), len(Udev[d]), len(Tdev[d]))
        I = I.iloc[:n]
        if t is not None: t = t.iloc[:n]
        for d in range(1, MAX_DEVICES + 1):
            Rdev[d] = Rdev[d].iloc[:n]
            Udev[d] = Udev[d].iloc[:n]
            Tdev[d] = Tdev[d].iloc[:n]

    return (t, I, Rdev, Udev, Tdev)

t_arr, I_arr, Rdev, Udev, Tdev = load_csv(CSV_PATH)

# ---------- Ticking thread ----------
def tick_from_csv():
    idx = 0
    n = len(I_arr)
    last_wall = time.time()
    last_t = float(t_arr.iloc[0]) if (USE_TIME_COLUMN and t_arr is not None and len(t_arr) > 0) else 0.0

    while True:
        # Update I (signed)
        i_int = enc_s16(I_arr.iloc[idx], I_SCALE)
        current_block.setI(i_int)

        # Update R/U/T for every device
        for dev_id, block in enumerate(hr_blocks, start=1):
            r_int = enc_u16(Rdev[dev_id].iloc[idx], R_SCALE)
            v_int = enc_u16(Udev[dev_id].iloc[idx], V_SCALE)
            t_int = enc_u16(Tdev[dev_id].iloc[idx], T_SCALE)
            block.set3(r_int, v_int, t_int)

        # pacing
        if USE_TIME_COLUMN and t_arr is not None and len(t_arr) > 0:
            if idx + 1 < n:
                next_t = float(t_arr.iloc[idx + 1])
                dt = max(0.0, next_t - last_t)
            else:
                dt = 1.0
            now = time.time()
            time.sleep(max(0.0, (last_wall + dt) - now))
            last_wall = time.time()
            last_t += dt
        else:
            time.sleep(1.0)

        idx += 1
        if idx >= n:
            if LOOP_CSV:
                idx = 0
                if USE_TIME_COLUMN and t_arr is not None and len(t_arr) > 0:
                    last_t = float(t_arr.iloc[0])
                    last_wall = time.time()
            else:
                break

# ---------- Optional logger ----------
def console_logger(sample_every=1.0):
    print("YYYYMMDD,hhmmss,unix,dev, R(ohm), U(V), T(C), I(A)", flush=True)
    while True:
        now = datetime.datetime.now()
        dev = 1
        print(f"{now:%Y%m%d},{now:%H%M%S},{int(now.timestamp())},{dev},"
              f"{Rdev[dev].iloc[0]:.6f},{Udev[dev].iloc[0]:.3f},{Tdev[dev].iloc[0]:.2f},{I_arr.iloc[0]:.3f}",
              flush=True)
        time.sleep(sample_every)

if __name__ == "__main__":
    threading.Thread(target=tick_from_csv, daemon=True).start()
    # threading.Thread(target=console_logger, daemon=True).start()
    StartSerialServer(context, port=PORT, baudrate=BAUD, bytesize=8, parity="N", stopbits=1)
