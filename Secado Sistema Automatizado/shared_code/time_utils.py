"""
Utilidades para normalizar timestamps a la zona horaria de Uruguay.
"""

import logging
from typing import Optional

import pandas as pd
import pytz

logger = logging.getLogger(__name__)

TZ_URU = pytz.timezone("America/Montevideo")


def normalize_timestamp(
    df: pd.DataFrame,
    col: str = "timestamp",
    assume_local: bool = False,
) -> pd.DataFrame:
    """
    Normaliza una columna timestamp a naive UTC-3 (Montevideo).

    - Si es timezone-aware → convierte a Montevideo y quita tz.
    - Si es naive y assume_local=True → se asume hora local y se vuelve naive.
    - Si es naive y assume_local=False → se deja como está (ya se asume en local).
    """
    if col not in df.columns:
        return df

    try:
        ts = pd.to_datetime(df[col], errors="coerce")
    except Exception as exc:
        logger.warning("normalize_timestamp: No se pudo parsear %s: %s", col, exc)
        return df

    df[col] = ts
    if ts.isna().all():
        return df

    try:
        tzinfo: Optional[pytz.BaseTzInfo] = ts.dt.tz  # type: ignore[attr-defined]
    except AttributeError:
        tzinfo = None

    try:
        if tzinfo is not None:
            df[col] = ts.dt.tz_convert(TZ_URU).dt.tz_localize(None)
        else:
            if assume_local:
                localized = ts.dt.tz_localize(TZ_URU, ambiguous="infer", nonexistent="shift_forward")
                df[col] = localized.dt.tz_localize(None)
            else:
                df[col] = ts
    except Exception as exc:
        logger.warning("normalize_timestamp: Error normalizando %s: %s", col, exc)
    return df


__all__ = ["normalize_timestamp", "TZ_URU"]

