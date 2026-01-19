"""
Tareas asíncronas para Django-Q.

Este archivo contiene todas las funciones que Django-Q puede ejecutar.
Separadas del servicio principal para mantener separación de responsabilidades.
"""
from __future__ import annotations

from asgiref.sync import async_to_sync

from core.services.ReportePaywayService import ReportePaywayService
from core.services.ReporteVtexService import ReporteVtexService
from core.services.ReporteCDPService import ReporteCDPService
from core.services.ReporteJanisService import ReporteJanisService
from core.services.CruceService import CruceService
from core.models import ReportePayway, ReporteVtex, ReporteCDP, ReporteJanis, Cruce
from django.conf import settings
import logging
import os

logger: logging.Logger = logging.getLogger(__name__)


# ============================================================================
# TAREA PRINCIPAL: Generar reporte de forma asíncrona
# ============================================================================

def generar_reporte_payway_async(fecha_inicio: str, fecha_fin: str, reporte_id: int, ruta_carpeta: str | None = None) -> int:
    """
    Genera un reporte de Payway de forma asíncrona.

    Esta función está diseñada para ser ejecutada por Django-Q workers.

    Args:
        fecha_inicio (str): Fecha de inicio en formato DD/MM/YYYY
        fecha_fin (str): Fecha de fin en formato DD/MM/YYYY
        reporte_id (int): ID del reporte creado en la base de datos
        ruta_carpeta (str, optional): Ruta donde guardar archivos.
                                      Si es None, usa MEDIA_ROOT/reportes_payway

    Returns:
        int: ID del reporte generado

    Raises:
        ValueError: Si las fechas son inválidas o faltan credenciales
        Exception: Si ocurre algún error durante la generación

    Ejemplo de uso desde view:
        from django_q.tasks import async_task

        task_id = async_task(
            'core.tasks.generar_reporte_payway_async',
            '01/12/2024',
            '10/12/2024',
            reporte_id,
            hook='core.tasks.notificar_reporte_completado'
        )
    """
    logger.info(f"[Django-Q] Iniciando generación asíncrona: {fecha_inicio} - {fecha_fin}")

    try:
        # Obtener el reporte de la base de datos
        nuevo_reporte = ReportePayway.objects.get(id=reporte_id)

        # Configurar ruta si no se proporcionó
        if ruta_carpeta is None:
            ruta_carpeta = os.path.join(settings.MEDIA_ROOT, 'reportes_payway')

        # Instanciar el servicio
        servicio = ReportePaywayService(ruta_carpeta=ruta_carpeta)
        reporte_id = async_to_sync(servicio.generar_reporte)(
            fecha_inicio,
            fecha_fin,
            nuevo_reporte
        )
        # Ejecutar la generación (puede tardar minutos u horas)

        logger.info(f"[Django-Q] Reporte {reporte_id} generado exitosamente")
        return reporte_id

    except Exception as e:
        logger.error(f"[Django-Q] Error al generar reporte Payway: {e}", exc_info=True)
        raise


def generar_reporte_vtex_async(fecha_inicio: str, fecha_fin: str, reporte_id: int, ruta_carpeta: str | None = None) -> int:
    """
    Genera un reporte de VTEX de forma asíncrona.

    Esta función está diseñada para ser ejecutada por Django-Q workers.
    Los filtros se obtienen automáticamente del reporte mediante FiltroReporteVtex.

    Args:
        fecha_inicio (str): Fecha de inicio en formato DD/MM/YYYY
        fecha_fin (str): Fecha de fin en formato DD/MM/YYYY
        reporte_id (int): ID del reporte creado en la base de datos
        ruta_carpeta (str, optional): Ruta donde guardar archivos.
                                      Si es None, usa MEDIA_ROOT/reportes_vtex

    Returns:
        int: ID del reporte generado

    Raises:
        ValueError: Si las fechas son inválidas
        Exception: Si ocurre algún error durante la generación

    Ejemplo de uso desde view:
        from django_q.tasks import async_task

        task_id = async_task(
            'core.tasks.generar_reporte_vtex_async',
            '01/12/2024',
            '10/12/2024',
            reporte_id
        )
    """
    logger.info(f"[Django-Q] Iniciando generación asíncrona VTEX: {fecha_inicio} - {fecha_fin}")

    try:
        # Obtener el reporte de la base de datos
        reporte = ReporteVtex.objects.get(id=reporte_id)

        # Configurar ruta si no se proporcionó
        if ruta_carpeta is None:
            ruta_carpeta = os.path.join(settings.MEDIA_ROOT, 'reportes_vtex')

        # Instanciar el servicio
        servicio = ReporteVtexService(ruta_carpeta=ruta_carpeta)

        # Ejecutar la generación (los filtros se obtienen del reporte)
        resultado = async_to_sync(servicio.generar_reporte)(
            fecha_inicio,
            fecha_fin,
            reporte_id
        )

        logger.info(f"[Django-Q] Reporte VTEX #{reporte_id} generado exitosamente")
        return reporte_id

    except Exception as e:
        logger.error(f"[Django-Q] Error al generar reporte VTEX: {e}", exc_info=True)
        raise


def generar_reporte_cdp_async(fecha_inicio: str, fecha_fin: str, reporte_id: int, ruta_carpeta: str | None = None) -> int:
    """
    Genera un reporte de CDP de forma asíncrona.

    Esta función está diseñada para ser ejecutada por Django-Q workers.

    Args:
        fecha_inicio (str): Fecha de inicio en formato DD/MM/YYYY
        fecha_fin (str): Fecha de fin en formato DD/MM/YYYY
        reporte_id (int): ID del reporte creado en la base de datos
        ruta_carpeta (str, optional): Ruta donde guardar archivos.
                                      Si es None, usa MEDIA_ROOT/reportes_cdp

    Returns:
        int: ID del reporte generado

    Raises:
        ValueError: Si las fechas son inválidas o faltan credenciales
        Exception: Si ocurre algún error durante la generación

    Ejemplo de uso desde view:
        from django_q.tasks import async_task

        task_id = async_task(
            'core.tasks.generar_reporte_cdp_async',
            '01/12/2024',
            '10/12/2024',
            reporte_id
        )
    """
    logger.info(f"[Django-Q] Iniciando generación asíncrona CDP: {fecha_inicio} - {fecha_fin}")

    try:
        # Obtener el reporte de la base de datos
        reporte = ReporteCDP.objects.get(id=reporte_id)

        # Configurar ruta si no se proporcionó
        if ruta_carpeta is None:
            ruta_carpeta = os.path.join(settings.MEDIA_ROOT, 'reportes_cdp')

        # Instanciar el servicio
        servicio = ReporteCDPService(ruta_carpeta=ruta_carpeta)

        # Ejecutar la generación (puede tardar minutos u horas)
        resultado = async_to_sync(servicio.generar_reporte)(
            fecha_inicio,
            fecha_fin,
            reporte_id
        )

        logger.info(f"[Django-Q] Reporte CDP #{reporte_id} generado exitosamente")
        return reporte_id

    except Exception as e:
        logger.error(f"[Django-Q] Error al generar reporte CDP: {e}", exc_info=True)
        raise


def generar_reporte_janis_async(fecha_inicio: str, fecha_fin: str, reporte_id: int, ruta_carpeta: str | None = None) -> int:
    """
    Genera un reporte de Janis de forma asíncrona.

    Esta función está diseñada para ser ejecutada por Django-Q workers.

    Args:
        fecha_inicio (str): Fecha de inicio en formato DD/MM/YYYY
        fecha_fin (str): Fecha de fin en formato DD/MM/YYYY
        reporte_id (int): ID del reporte creado en la base de datos
        ruta_carpeta (str, optional): Ruta donde guardar archivos.
                                      Si es None, usa MEDIA_ROOT/reportes_janis

    Returns:
        int: ID del reporte generado

    Raises:
        ValueError: Si las fechas son inválidas o faltan credenciales
        Exception: Si ocurre algún error durante la generación

    Ejemplo de uso desde view:
        from django_q.tasks import async_task

        task_id = async_task(
            'core.tasks.generar_reporte_janis_async',
            '01/12/2024',
            '10/12/2024',
            reporte_id
        )
    """
    logger.info(f"[Django-Q] Iniciando generación asíncrona Janis: {fecha_inicio} - {fecha_fin}")

    try:
        # Obtener el reporte de la base de datos
        reporte = ReporteJanis.objects.get(id=reporte_id)

        # Configurar ruta si no se proporcionó
        if ruta_carpeta is None:
            ruta_carpeta = os.path.join(settings.MEDIA_ROOT, 'reportes_janis')

        # Instanciar el servicio
        servicio = ReporteJanisService(ruta_carpeta=ruta_carpeta)

        # Ejecutar la generación (puede tardar minutos u horas)
        resultado = async_to_sync(servicio.generar_reporte)(
            fecha_inicio,
            fecha_fin,
            reporte_id
        )

        logger.info(f"[Django-Q] Reporte Janis #{reporte_id} generado exitosamente")
        return reporte_id

    except Exception as e:
        logger.error(f"[Django-Q] Error al generar reporte Janis: {e}", exc_info=True)
        raise


def generar_cruce_async(cruce_id: int, reporte_vtex_id: int | None = None, reporte_payway_id: int | None = None, reporte_cdp_id: int | None = None, reporte_janis_id: int | None = None) -> int:
    """
    Genera un cruce de reportes de forma asíncrona.

    Esta función está diseñada para ser ejecutada por Django-Q workers.

    Args:
        cruce_id (int): ID del cruce creado en la base de datos
        reporte_vtex_id (int, optional): ID del reporte VTEX a incluir
        reporte_payway_id (int, optional): ID del reporte Payway a incluir
        reporte_cdp_id (int, optional): ID del reporte CDP a incluir
        reporte_janis_id (int, optional): ID del reporte Janis a incluir

    Returns:
        int: ID del cruce generado

    Ejemplo de uso desde view:
        from django_q.tasks import async_task

        task_id = async_task(
            'core.tasks.generar_cruce_async',
            cruce_id,
            reporte_vtex_id,
            reporte_payway_id,
            reporte_cdp_id,
            reporte_janis_id
        )
    """
    logger.info(f"[Django-Q] Iniciando generación asíncrona de cruce #{cruce_id}")

    try:
        # Instanciar el servicio
        servicio = CruceService()

        # Ejecutar la generación
        resultado = async_to_sync(servicio.generar_cruce)(
            cruce_id,
            reporte_vtex_id,
            reporte_payway_id,
            reporte_cdp_id,
            reporte_janis_id
        )

        logger.info(f"[Django-Q] Cruce #{cruce_id} generado exitosamente")
        return cruce_id

    except Exception as e:
        logger.error(f"[Django-Q] Error al generar cruce: {e}", exc_info=True)
        raise
