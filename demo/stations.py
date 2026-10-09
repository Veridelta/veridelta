# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Write the weather station files that the `stations-` tapes for video read.

Four made-up stations report one reading a day for January 2025. A legacy IDL
pipeline wrote `stations_idl.csv`, and its Python port writes
`stations_python.csv`. The two differ in the ways a real port does:

- IDL computes in 32-bit floats by default, so its humidity, written with six
  decimals, carries the float's error, such as `65.300003` for `65.3`. The
  port computes in 64-bit floats and writes `65.3`.
- IDL wrote `-999` for a missing humidity, and the port writes nothing.
- The port renamed `temp` to `temperature_c`.
- IDL used the wrong elevation for station S3, so its pressure there reads
  12.1 hPa low. The port fixes that on purpose.
- The port has a bug of its own: four of S1's readings below zero lost their
  minus sign. `stations_python_fixed.csv` is the port with that bug fixed.

Temperature and pressure are written to one decimal on both sides, so they
differ only where the fix and the bug make them differ. Nothing here is
random: run it again, from the repository root, and it writes the same files.

    python demo/stations.py
"""

import math
import struct
from datetime import date, timedelta
from pathlib import Path

_DEMO = Path(__file__).resolve().parent
_DAYS = [date(2025, 1, 1) + timedelta(days=day) for day in range(31)]
_STATIONS = {
    # name: (mean temperature, pressure, humidity)
    "S1": (-1.6, 1018.7, 83.4),
    "S2": (8.4, 1016.2, 71.3),
    "S3": (2.9, 1009.4, 66.2),
    "S4": (4.7, 1012.8, 78.6),
}
_MISSING = {("S1", 2), ("S2", 11), ("S3", 19), ("S4", 27), ("S2", 24)}
"""The (station, day) readings whose humidity the sensor did not record."""
_ELEVATION_ERROR = 12.1
"""How far below the true pressure IDL put station S3, from its wrong elevation."""
_LOST_SIGNS = 4
"""How many of S1's readings below zero the port writes without their minus sign."""


def _float32(value: float) -> float:
    """Return the 32-bit float nearest a value, as IDL holds it by default."""
    return float(struct.unpack("f", struct.pack("f", value))[0])


def _humidity(mean: float, day: int) -> float:
    """Return a humidity to one decimal whose 32-bit value shows its error at six decimals."""
    value = round(mean + 9 * math.cos(day / 3.1), 1)
    while f"{_float32(value):.6f}" == f"{value:.6f}":
        value = round(value + 0.1, 1)
    return value


def _readings() -> list[tuple[str, date, float, float, float | None]]:
    """Return every true reading: station, date, temperature, pressure, and humidity."""
    rows = []
    for station, (temperature, pressure, humidity) in _STATIONS.items():
        for index, day in enumerate(_DAYS, start=1):
            rows.append(
                (
                    station,
                    day,
                    round(temperature - 4.5 * math.sin(index / 2.3), 1),
                    round(pressure + 6 * math.sin(index / 4.7), 1),
                    None if (station, index) in _MISSING else _humidity(humidity, index),
                )
            )
    return rows


def _write(path: Path, header: str, lines: list[str]) -> None:
    """Write a CSV file with Unix line endings."""
    path.write_text(header + "\n" + "\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def main(folder: Path = _DEMO) -> None:
    """Write the IDL output, the Python port, and the fixed port into a folder."""
    rows = _readings()
    idl = [
        f"{station},{day},{temperature:.1f},"
        f"{pressure - (_ELEVATION_ERROR if station == 'S3' else 0):.1f},"
        f"{-999.0 if humidity is None else _float32(humidity):.6f}"
        for station, day, temperature, pressure, humidity in rows
    ]
    unsigned = [row for row in rows if row[0] == "S1" and row[2] < 0][:_LOST_SIGNS]
    port, fixed = [], []
    for row in rows:
        station, day, temperature, pressure, humidity = row
        written = "" if humidity is None else f"{humidity:.1f}"
        fixed.append(f"{station},{day},{temperature:.1f},{pressure:.1f},{written}")
        shown = abs(temperature) if row in unsigned else temperature
        port.append(f"{station},{day},{shown:.1f},{pressure:.1f},{written}")

    _write(folder / "stations_idl.csv", "station,obs_date,temp,pressure_hpa,humidity", idl)
    python_header = "station,obs_date,temperature_c,pressure_hpa,humidity"
    _write(folder / "stations_python.csv", python_header, port)
    _write(folder / "stations_python_fixed.csv", python_header, fixed)


if __name__ == "__main__":
    main()
