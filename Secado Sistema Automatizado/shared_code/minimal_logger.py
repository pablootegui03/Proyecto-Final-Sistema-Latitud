"""
Sistema de logging minimalista para Azure Functions sin costos adicionales.

Usa print() nativo (gratis) en lugar de Application Insights.
Formato ultra-compacto: [TIMESTAMP] [NIVEL] [ETAPA] mensaje_breve

Activar modo DEBUG: Configurar variable de entorno DEBUG_MODE=true en Azure Portal
"""

import os
import traceback
from datetime import datetime, timezone
from typing import Optional


# Variable de entorno para activar logs detallados
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"


def _timestamp() -> str:
    """Retorna timestamp compacto: YYYY-MM-DD HH:MM:SS"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str, level: str = "INFO", stage: Optional[str] = None) -> None:
    """
    Log ultra-compacto (máximo 200 caracteres por línea).
    
    Args:
        msg: Mensaje breve (máximo 150 caracteres)
        level: INFO, ERROR, WARN, DEBUG
        stage: Etapa del proceso (opcional, ej: "AUTH", "DOWNLOAD", "PARSE")
    """
    # Truncar mensaje si es muy largo
    if len(msg) > 150:
        msg = msg[:147] + "..."
    
    # Construir línea compacta
    parts = [f"[{_timestamp()}]", f"[{level}]"]
    if stage:
        parts.append(f"[{stage}]")
    parts.append(msg)
    
    # Print nativo (va directo a logs de Azure, gratis)
    print(" ".join(parts))


def log_start(func_name: str, **kwargs) -> None:
    """Log de inicio de función."""
    params = " ".join([f"{k}={v}" for k, v in kwargs.items() if v][:3])
    log(f"START func={func_name} {params}".strip(), "INFO", "START")


def log_end(func_name: str, status: str, duration_sec: float, **kwargs) -> None:
    """Log de fin de función."""
    params = " ".join([f"{k}={v}" for k, v in kwargs.items() if v][:2])
    log(f"END func={func_name} status={status} time={duration_sec:.1f}s {params}".strip(), 
        "INFO" if status == "OK" else "ERROR", "END")


def log_error(stage: str, error: Exception, context: Optional[dict] = None) -> None:
    """
    Log de error compacto.
    
    Args:
        stage: Etapa donde ocurrió (ej: "AUTH", "DOWNLOAD", "PARSE")
        error: Excepción capturada
        context: Diccionario con contexto adicional (solo claves importantes)
    """
    error_msg = str(error)[:80]  # Truncar mensaje largo
    ctx_str = ""
    if context:
        ctx_parts = [f"{k}={v}" for k, v in context.items() if v][:2]
        ctx_str = " " + " ".join(ctx_parts)
    
    log(f"ERROR stage={stage} error={error_msg}{ctx_str}".strip(), "ERROR", stage)
    
    # Solo en modo DEBUG: stack trace completo
    if DEBUG_MODE:
        log(f"STACK: {traceback.format_exc()[:500]}", "DEBUG", stage)


def log_debug(msg: str, stage: Optional[str] = None, **kwargs) -> None:
    """Log detallado solo si DEBUG_MODE=true."""
    if not DEBUG_MODE:
        return
    
    params = " ".join([f"{k}={v}" for k, v in kwargs.items() if v][:3])
    full_msg = f"{msg} {params}".strip()
    log(full_msg, "DEBUG", stage)

